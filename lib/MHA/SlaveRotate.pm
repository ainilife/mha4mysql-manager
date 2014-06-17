#!/usr/bin/env perl

#  Copyright (C) 2011 DeNA Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

package MHA::SlaveRotate;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Carp qw(croak);
use Getopt::Long;
use Pod::Usage;
use Log::Dispatch;
use Log::Dispatch::Screen;
use Log::Dispatch::File;
use MHA::Config;
use MHA::ServerManager;
use MHA::Server;
use MHA::ManagerUtil;
use File::Basename;
use Parallel::ForkManager;
use Sys::Hostname;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_logfile;
my $g_workdir;
my $g_remove_slave_conf=0;
my $g_interactive = 1;
my $g_ignore_last_failover;
my $g_last_failover_minute   = 480;
my $g_wait_on_failover_error = 0;
my $_server_manager;
my $start_datetime;
my %_rotate_slave_arg;
my $log;
my $mail_subject;
my $mail_body;
my $_status_handler;
my $_failover_complete_file;
my $_failover_error_file;
my $_create_error_file = 0;
my $g_disable_dead_slave_conf=1;

sub init_config() {
  $log = MHA::ManagerUtil::init_log($g_logfile);

  my ( $sc_ref, $binlog_ref ) = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  )->read_config();
  my @servers_config        = @$sc_ref;
  my @binlog_servers_config = @$binlog_ref;

  if ( !$g_logfile
    && !$g_interactive
    && $servers_config[0]->{manager_log} )
  {
    $g_logfile = $servers_config[0]->{manager_log};
  }
  $log =
    MHA::ManagerUtil::init_log( $g_logfile, $servers_config[0]->{log_level} );
  $log->info("MHA::SlaveFailover version $MHA::ManagerConst::VERSION.");

  unless ($g_workdir) {
    if ( $servers_config[0]->{manager_workdir} ) {
      $g_workdir = $servers_config[0]->{manager_workdir};
    }
    else {
      $g_workdir = "/var/tmp";
    }
  }
  return @servers_config;
}

sub force_shutdown_internal($) {
  my $rotate_slave = shift;

  $log->info(
"Forcing shutdown so that applications never connect to the current slave.."
  );

  if ( $rotate_slave->{slave_ip_online_change_script} ) {
    my $command =
"$rotate_slave->{slave_ip_online_change_script} --orig_slave_host=$rotate_slave->{hostname} --orig_slave_ip=$rotate_slave->{ip} --orig_slave_port=$rotate_slave->{port}";
  $command .= " --command=stop";
    $log->info("Executing slave IP deactivatation script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" done.");
      $mail_body .=
        "Invalidated slave IP address on $rotate_slave->{hostname}.\n";
    }
    else {
      my $message =
        "Failed to deactivate slave IP with return code $high:$low";
      $log->error($message);
      $mail_body .= $message . "\n";
      if ( $high == 10 ) {
        $log->warning("Proceeding.");
      }
      else {
        croak;
      }
    }
  }
  else {
    $log->warning(
"slave_ip_online_change_script is not set. Skipping invalidating dead slave ip address."
    );
  }

  return 0;
}

sub force_shutdown_slave($) {
  my $rotate_slave = shift;

  my $appname      = $_status_handler->{basename};
  my @alive_slaves = $_server_manager->get_alive_slaves();
  $mail_subject = $appname . ": MySQL Master failover $rotate_slave->{hostname}";
  $mail_body    = "Slave $rotate_slave->{hostname} is down!\n\n";

  $mail_body .= "Check MHA Manager logs at " . hostname();
  $mail_body .= ":$g_logfile" if ($g_logfile);
  $mail_body .= " for details.\n\n";
  if ($g_interactive) {
    $mail_body .= "Started manual(interactive) failover.\n";
  }
  else {
    $mail_body .= "Started automated(non-interactive) failover.\n";
  }

  force_shutdown_internal($rotate_slave);

  return 0;
}

sub check_settings($) {
  my $servers_config_ref = shift;
  my @servers_config     = @$servers_config_ref;
  my $rotate_slave;
  MHA::ManagerUtil::check_node_version($log);
  $_status_handler =
    new MHA::FileStatus( conffile => $g_config_file, dir => $g_workdir );
  $_status_handler->init();
  my $appname = $_status_handler->{basename};
  $_failover_complete_file = "$g_workdir/$appname.failover.complete";
  $_failover_error_file    = "$g_workdir/$appname.failover.error";

  $_status_handler->update_status($MHA::ManagerConst::ST_FAILOVER_RUNNING_S);

  $_server_manager = new MHA::ServerManager( servers => \@servers_config );
  $_server_manager->set_logger($log);
  if($g_interactive){
    $_server_manager->connect_all_and_read_server_status();
  }else{
    $log->debug(
      "Skipping connecting to dead slave $_rotate_slave_arg{hostname}.");
    $_server_manager->connect_all_and_read_server_status(
      $_rotate_slave_arg{hostname},
      $_rotate_slave_arg{ip}, $_rotate_slave_arg{port} );
  }

  my @dead_servers  = $_server_manager->get_dead_servers();
  my @alive_servers = $_server_manager->get_alive_servers();
  my @alive_slaves  = $_server_manager->get_alive_slaves();
  my $master        = $_server_manager->validate_current_master();

  if ( $_rotate_slave_arg{hostname} eq $master->{hostname}
            && $_rotate_slave_arg{port} eq $master->{port} ) {
    $log->error(
      "The Node: $_rotate_slave_arg{hostname}:$_rotate_slave_arg{port} is current master. Stop mark-off. ");
    $log->error(
      "Please use <mha_control set_offline master> instead.");
    croak;
  }

  $log->info("Dead Servers:");
  $_server_manager->print_dead_servers();
  if ( $#alive_servers < 1 ) {
    $log->error("There is no alive server. Stop mark-off");
    croak;
  }

  my $rotate_slave_alive = 0;
  foreach my $d (@alive_slaves) {
    if ( $d->{hostname} eq $_rotate_slave_arg{hostname} && $d->{port} eq $_rotate_slave_arg{port}) {
      $rotate_slave_alive = 1;
      $rotate_slave       = $d;
      last;
    }
  }
  unless ($rotate_slave_alive) {
    $log->error(
      "The slave $_rotate_slave_arg{hostname} is alread dead/offline. Stop mark-off.");
    croak;
  }

  # quick check that the dead server is really dead
  $log->info("Checking slave reachability via mysql(double check)..");
    my $rc = MHA::DBHelper::check_connection_fast_util(
      $rotate_slave->{hostname}, $rotate_slave->{port},
      $rotate_slave->{user},     $rotate_slave->{password}
    );
  if ( ! $rc )
  {
    $log->error(
      sprintf(
        "The slave %s is not reachable via mysql (error=%s) ! Stop mark-off.",
        $rotate_slave->get_hostinfo(), $rc
      )
    );
    croak;
  }
  $log->info(" ok.");

  $log->info("Alive Servers:");
  $_server_manager->print_alive_servers();
  $log->info("Alive Slaves:");
  $_server_manager->print_alive_slaves();
  $_server_manager->print_failed_slaves_if();
  $_server_manager->print_unmanaged_slaves_if();

  # Checking last failover error file
  if ($g_ignore_last_failover) {
    MHA::NodeUtil::drop_file_if($_failover_error_file);
    MHA::NodeUtil::drop_file_if($_failover_complete_file);
  }
  if ( -f $_failover_error_file ) {
    my $message =
        "Failover error flag file $_failover_error_file "
      . "exists. This means the last failover failed. Check error logs "
      . "for detail, fix problems, remove $_failover_error_file, "
      . "and restart this script.";
    $log->error($message);
    croak;
  }

  if ($g_interactive) {
    print "Slave $rotate_slave->{hostname}:$rotate_slave->{port} is alive. Proceed? (yes/NO): ";
    my $ret = <STDIN>;
    chomp($ret);
    die "Stopping mark-off." if ( lc($ret) !~ /y/ );
  }

  # If the last failover was done within 8 hours, we don't do failover
  # to avoid ping-pong
  if ( -f $_failover_complete_file ) {
    my $lastts       = ( stat($_failover_complete_file) )[9];
    my $current_time = time();
    if ( $current_time - $lastts < $g_last_failover_minute * 60 ) {
      my ( $sec, $min, $hh, $dd, $mm, $yy, $weak, $yday, $opt ) =
        localtime($lastts);
      $mm = $mm + 1;
      $yy = $yy + 1900;
      my $msg =
          "Last failover was done at "
        . "$yy/$mm/$dd $hh:$min:$sec."
        . " Current time is too early to do failover again. If you want to "
        . "do failover, manually remove $_failover_complete_file "
        . "and run this script again.";
      $log->error($msg);
      croak;
    }
    else {
      MHA::NodeUtil::drop_file_if($_failover_complete_file);
    }
  }
  $_server_manager->get_failover_advisory_locks();
  $_server_manager->start_sql_threads_if();
  return $rotate_slave;
}

sub do_slave_online_switch {
  my $error_code = 1;
  my $rotate_slave;

  eval {
    my @servers_config = init_config();
    $log->info("Starting slave failover.");
    $log->info();
    $log->info("* Phase 1: Configuration Check Phase..\n");
    $log->info();
    $rotate_slave = check_settings( \@servers_config );

    $log->info("** Phase 1: Configuration Check Phase completed.\n");
    $log->info();
    $log->info("* Phase 2: Dead Slave Shutdown Phase..\n");
    $log->info();
    $error_code = force_shutdown_slave($rotate_slave);

    if ( $g_disable_dead_slave_conf && $error_code == 0 ) {
      MHA::Config::disable_block_and_save( $g_config_file, $rotate_slave->{id},
        $log );
    }
    cleanup();
  };
  if ($@) {
    if ( $rotate_slave && $rotate_slave->{not_error} ) {
      $log->info($@);
    }
    else {
      MHA::ManagerUtil::print_error( "Got ERROR: $@", $log );
    }
    $_server_manager->disconnect_all() if ($_server_manager);
    undef $@;
  }

  return $error_code;
}

sub cleanup {
  $_server_manager->release_failover_advisory_lock();
  $_server_manager->disconnect_all();
  MHA::NodeUtil::create_file_if($_failover_complete_file);
  $_create_error_file = 0;
  return 0;
}

sub finalize_on_error {
  eval {

    # Failover failure happened
    $_status_handler->update_status($MHA::ManagerConst::ST_FAILOVER_ERROR_S)
      if ($_status_handler);
    if ( $g_wait_on_failover_error > 0 && !$g_interactive ) {
      if ($log) {
        $log->info(
          "Waiting for $g_wait_on_failover_error seconds for error exit..");
      }
      else {
        print
          "Waiting for $g_wait_on_failover_error seconds for error exit..\n";
      }
      sleep $g_wait_on_failover_error;
    }
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      if ($_status_handler);
  };
  if ($@) {
    MHA::ManagerUtil::print_error(
      "Got Error on finalize_on_error at failover: $@", $log );
    undef $@;
  }

}


sub main {
  local $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = \&exit_by_signal;
  local @ARGV = @_;
  my ( $slave_host, $slave_ip, $slave_port, $error_code );
  my $a = GetOptions(
    'global_conf=s'            => \$g_global_config_file,
    'conf=s'                   => \$g_config_file,
    'workdir=s'                => \$g_workdir,
    'manager_workdir=s'        => \$g_workdir,
    'interactive=i'            => \$g_interactive,
    'remove_slave_conf'        => \$g_remove_slave_conf,
    'rotate_slave_host=s'       => \$slave_host,
    'rotate_slave_ip=s'         => \$slave_ip,
    'rotate_slave_port=i'       => \$slave_port,
    'ignore_last_failover'     => \$g_ignore_last_failover,
    'disable_dead_slave_conf'  => \$g_disable_dead_slave_conf,
    'log_output=s'             => \$g_logfile,
  );
  if ( $#ARGV >= 0 ) {
    print "Unknown options: ";
    print $_ . " " foreach (@ARGV);
    print "\n";
    return 1;
  }
  unless ($g_config_file) {
    print "--conf=<server_config_file> must be set.\n";
    return 1;
  }
  unless ($slave_host) {
    print "--rotate_slave_host=<rotate_slave_host> must be set.\n";
    return 1;
  }
  unless ($slave_ip) {
    $slave_ip = MHA::NodeUtil::get_ip($slave_host);
    print "--rotate_slave_ip=<rotate_slave_ip> is not set. Using $slave_ip.\n";
  }
  unless ($slave_port) {
    $slave_port = 3306;
    print
      "--rotate_slave_port=<rotate_slave_port> is not set. Using $slave_port.\n";
  }

  $_rotate_slave_arg{hostname} = $slave_host;
  $_rotate_slave_arg{ip}       = $slave_ip;
  $_rotate_slave_arg{port}     = $slave_port;
  $g_logfile = undef if ($g_interactive);
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;
  eval { $error_code = do_slave_online_switch(); };
  if ($@) {
    $error_code = 1;
  }
  if ($error_code) {
    finalize_on_error();
  }
  return $error_code;
}

1;

