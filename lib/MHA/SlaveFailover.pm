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

package MHA::SlaveFailover;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Carp qw(croak);
use Getopt::Long qw(:config pass_through);
use Log::Dispatch;
use Log::Dispatch::File;
use MHA::NodeUtil;
use MHA::Config;
use MHA::ServerManager;
use MHA::FileStatus;
use MHA::ManagerUtil;
use MHA::ManagerConst;
use MHA::HealthCheck;
use File::Basename;
use Parallel::ForkManager;
use Sys::Hostname;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_interactive     = 1;
my $g_ssh_reachable   = 2;
my $g_workdir;
my $g_logfile;
my $g_last_failover_minute   = 480;
my $g_wait_on_failover_error = 0;
my $g_ignore_last_failover;
my $g_disable_dead_slave_conf=1;
my $_real_ssh_reachable;
my $_saved_file_suffix;
my $_start_datetime;
my $_failover_complete_file;
my $_failover_error_file;
my %_dead_slave_arg;
my $_server_manager;
my $_diff_binary_log;
my $_diff_binary_log_basename;
my $_has_saved_binlog = 0;
my $_status_handler;
my $_create_error_file = 0;
my $log;
my $mail_subject;
my $mail_body;
my $GEN_DIFF_OK = 15;

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

sub check_settings($) {
  my $servers_config_ref = shift;
  my @servers_config     = @$servers_config_ref;
  my $dead_slave;
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
      "Skipping connecting to dead slave $_dead_slave_arg{hostname}.");
    $_server_manager->connect_all_and_read_server_status(
      $_dead_slave_arg{hostname},
      $_dead_slave_arg{ip}, $_dead_slave_arg{port} );
  }

  my @dead_servers  = $_server_manager->get_dead_servers();
  my @alive_servers = $_server_manager->get_alive_servers();
  my @alive_slaves  = $_server_manager->get_alive_slaves();
  my $master_server = $_server_manager->get_orig_master();

  if( $_dead_slave_arg{hostname} eq $master_server->{hostname} 
        && $_dead_slave_arg{port} eq $master_server->{port} ){
    $log->error(
      "The dead slave is actually master, you should use <mha_control \@app force_offline master> instead.");
    $log->error("Stop failover.");
    croak;
  }


  #Make sure that dead server is current master only
  $log->info("Dead Servers:");
  $_server_manager->print_dead_servers();

  my $dead_slave_found = 0;
  foreach my $d (@dead_servers) {
    if ( $d->{hostname} eq $_dead_slave_arg{hostname} 
            && $d->{port} eq $_dead_slave_arg{port} ) {
      $dead_slave_found = 1;
      $dead_slave       = $d;
      last;
    }
  }
  unless ($dead_slave_found) {
    $log->error(
      "The slave $_dead_slave_arg{hostname} is not dead. Stop failover.");
    croak;
  }

  # quick check that the dead server is really dead
  $log->info("Checking slave reachability via mysql(double check)..");
  if (
    my $rc = MHA::DBHelper::check_connection_fast_util(
      $dead_slave->{hostname}, $dead_slave->{port},
      $dead_slave->{user},     $dead_slave->{password}
    )
    )
  {
    $log->error(
      sprintf(
        "The slave %s is reachable via mysql (error=%s) ! Stop failover.",
        $dead_slave->get_hostinfo(), $rc
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
    print "Slave "
      . $dead_slave->get_hostinfo()
      . " is dead. Proceed? (yes/NO): ";
    my $ret = <STDIN>;
    chomp($ret);
    die "Stopping failover." if ( lc($ret) !~ /y/ );
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
      if ($g_interactive) {
          print "Last failover was done at "
            . "$yy/$mm/$dd $hh:$min:$sec."
              . " Maybe it's too early to do failover again. "
              . " Are sure to Proceed? (yes/NO): ";
        my $ret = <STDIN>;
        chomp($ret);
        if ( lc($ret) !~ /^y/ ){
            die "Stopping failover." 
        }else{
          MHA::NodeUtil::drop_file_if($_failover_complete_file);
        }
      }else{
          my $msg =
              "Last failover was done at "
            . "$yy/$mm/$dd $hh:$min:$sec."
            . " Current time is too early to do failover again. If you want to "
            . "do failover, manually remove $_failover_complete_file "
            . "and run this script again.";
          $log->error($msg);
          croak;
      }
    }
    else {
      MHA::NodeUtil::drop_file_if($_failover_complete_file);
    }
  }
  $_server_manager->get_failover_advisory_locks();
  $_server_manager->start_sql_threads_if();
  return $dead_slave;
}

sub force_shutdown_internal($) {
  my $dead_slave = shift;

  $log->info(
"Forcing shutdown so that applications never connect to the current slave.."
  );

  if ( $dead_slave->{slave_ip_failover_script} ) {
    my $command =
"$dead_slave->{slave_ip_failover_script} --orig_slave_host=$dead_slave->{hostname} --orig_slave_ip=$dead_slave->{ip} --orig_slave_port=$dead_slave->{port}";
    if ( $_real_ssh_reachable == 1 ) {
      $command .=
        " --command=stopssh" . " --ssh_user=$dead_slave->{ssh_user} ";
    }
    else {
      $command .= " --command=stop";
    }
    $log->info("Executing slave IP deactivatation script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" done.");
      $mail_body .=
        sprintf("Invalidated slave IP address on %s.\n",$dead_slave->get_hostinfo());
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
"slave_ip_failover_script is not set. Skipping invalidating dead slave ip address."
    );
  }

=pod
  # force slave shutdown
  if ( $dead_slave->{shutdown_script} ) {
    my $command = "$dead_slave->{shutdown_script}";
    if ( $_real_ssh_reachable == 1 ) {
      $command .=
        " --command=stopssh" . " --ssh_user=$dead_slave->{ssh_user} ";
    }
    else {
      $command .= " --command=stop";
    }
    $command .=
" --host=$dead_slave->{hostname}  --ip=$dead_slave->{ip}  --port=$dead_slave->{port} ";
    $command .= " --pid_file=$dead_slave->{slave_pid_file}"
      if ( $dead_slave->{slave_pid_file} );
    $log->info("Executing SHUTDOWN script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" Power off done.");
      $mail_body .= "Power off $dead_slave->{hostname}.\n";
      $_real_ssh_reachable = 0;
    }
    else {
      if ( $high == 10 ) {
        $log->info(" SSH reachable. Shutting down mysqld done.");
        $mail_body .=
"SSH reachable on $dead_slave->{hostname}. Shutting down mysqld done.\n";
        $_real_ssh_reachable = 1;
      }
      else {
        my $message =
          "Failed to execute shutdown_script with return code $high:$low";
        $log->error($message);
        $mail_body .= $message . "\n";
        croak;
      }
    }
  }
  else {
    $log->warning(
"shutdown_script is not set. Skipping explicit shutting down of the dead slave."
    );
  }
=cut
  return 0;
}

sub force_shutdown_slave($) {
  my $dead_slave = shift;

  my $appname      = $_status_handler->{basename};
  my @alive_slaves = $_server_manager->get_alive_slaves();
  $mail_subject = sprintf("%s: MySQL Master failover %s", $appname, $dead_slave->get_hostinfo() );
  $mail_body    = sprintf("Slave %s is down!\n\n", $dead_slave->get_hostinfo() );

  $mail_body .= "Check MHA Manager logs at " . hostname();
  $mail_body .= ":$g_logfile" if ($g_logfile);
  $mail_body .= " for details.\n\n";
  if ($g_interactive) {
    $mail_body .= "Started manual(interactive) failover.\n";
  }
  else {
    $mail_body .= "Started automated(non-interactive) failover.\n";
  }

  $_real_ssh_reachable = $g_ssh_reachable;

  # SSH reachability is unknown. Verify here.
  if ( $_real_ssh_reachable >= 2 ) {
    if (
      MHA::HealthCheck::ssh_check_simple(
        $dead_slave->{ssh_user}, $dead_slave->{ssh_host},
        $dead_slave->{ssh_ip},   $dead_slave->{ssh_port},
        $dead_slave->{logger},   $dead_slave->{ssh_connection_timeout}
      )
      )
    {
      $_real_ssh_reachable = 0;
    }
    else {

      # additional check
      if (
        MHA::ManagerUtil::get_node_version(
          $dead_slave->{logger},   $dead_slave->{ssh_user},
          $dead_slave->{ssh_host}, $dead_slave->{ssh_ip},
          $dead_slave->{ssh_port}
        )
        )
      {
        $_real_ssh_reachable = 1;
      }
      else {
        $log->warning(
"Failed to get MHA Node version from dead slave. Guessing that SSH is NOT reachable."
        );
        $_real_ssh_reachable = 0;
      }
    }
  }
  force_shutdown_internal($dead_slave);

  return 0;
}

sub do_slave_failover {
  my $error_code = 1;
  my ( $dead_slave );

  eval {
    my @servers_config = init_config();
    $log->info("Starting slave failover.");
    $log->info();
    $log->info("* Phase 1: Configuration Check Phase..\n");
    $log->info();
    $dead_slave = check_settings( \@servers_config );

    $log->info("** Phase 1: Configuration Check Phase completed.\n");
    $log->info();
    $log->info("* Phase 2: Dead Slave Shutdown Phase..\n");
    $log->info();
    $error_code = force_shutdown_slave($dead_slave);

    if ( $g_disable_dead_slave_conf && $error_code == 0 ) {
      MHA::Config::disable_block_and_save( $g_config_file, $dead_slave->{id},
        $log );
    }
    cleanup();
  };
  if ($@) {
    if ( $dead_slave && $dead_slave->{not_error} ) {
      $log->info($@);
    }
    else {
      MHA::ManagerUtil::print_error( "Got ERROR: $@", $log );
      $mail_body .= "Got Error so couldn't continue failover from here.\n"
        if ($mail_body);
    }
    $_server_manager->disconnect_all() if ($_server_manager);
    undef $@;
  }
  eval {
    send_report( $dead_slave );
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      unless ($error_code);

    if ($_create_error_file) {
      MHA::NodeUtil::create_file_if($_failover_error_file);
    }
  };
  if ($@) {
    MHA::ManagerUtil::print_error( "Got ERROR on final reporting: $@", $log );
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

sub send_report {
  my $dead_slave = shift;

  if ( $mail_subject && $mail_body ) {
    $log->info( "\n\n"
        . "----- Failover Report -----\n\n"
        . $mail_subject . "\n\n"
        . $mail_body );
    if ( $dead_slave->{report_script} ) {
      my $command =
"$dead_slave->{report_script} --orig_slave_host=$dead_slave->{hostname} ";
      $command .= " --conf=$g_config_file ";
      $command .= " --subject=\"$mail_subject\" --body=\"$mail_body\"";
      $log->info("Sending mail..");
      my ( $high, $low ) =
        MHA::ManagerUtil::exec_system( $command, $g_logfile );
      if ( $high != 0 || $low != 0 ) {
        $log->error("Failed to send mail with return code $high:$low");
      }
    }
  }
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
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $_start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;

  GetOptions(
    'global_conf=s'            => \$g_global_config_file,
    'conf=s'                   => \$g_config_file,
    'dead_slave_host=s'       => \$slave_host,
    'dead_slave_ip=s'         => \$slave_ip,
    'dead_slave_port=i'       => \$slave_port,
    'interactive=i'            => \$g_interactive,
    'ssh_reachable=i'          => \$g_ssh_reachable,
    'last_failover_minute=i'   => \$g_last_failover_minute,
    'wait_on_failover_error=i' => \$g_wait_on_failover_error,
    'ignore_last_failover'     => \$g_ignore_last_failover,
    'workdir=s'                => \$g_workdir,
    'manager_workdir=s'        => \$g_workdir,
    'log_output=s'             => \$g_logfile,
    'manager_log=s'            => \$g_logfile,
    'disable_dead_slave_conf'  => \$g_disable_dead_slave_conf,
  );
  setpgrp( 0, $$ ) unless ($g_interactive);

  unless ($g_config_file) {
    print "--conf=<server_config_file> must be set.\n";
    return 1;
  }
  unless ($slave_host) {
    print "--dead_slave_host=<dead_slave_hostname> must be set.\n";
    return 1;
  }
  unless ($slave_ip) {
    $slave_ip = MHA::NodeUtil::get_ip($slave_host);
    print "--dead_slave_ip=<dead_slave_ip> is not set. Using $slave_ip.\n";
  }
  unless ($slave_port) {
    $slave_port = 3306;
    print
      "--dead_slave_port=<dead_slave_port> is not set. Using $slave_port.\n";
  }

  $_dead_slave_arg{hostname} = $slave_host;
  $_dead_slave_arg{ip}       = $slave_ip;
  $_dead_slave_arg{port}     = $slave_port;

  # in interactive mode, always prints to stdout/stderr
  $g_logfile = undef if ($g_interactive);

  eval { $error_code = do_slave_failover(); };
  if ($@) {
    $error_code = 1;
  }
  if ($error_code) {
    finalize_on_error();
  }
  return $error_code;
}

1;
