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

package MHA::ManagerShow;

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
use Parallel::ForkManager;
use Sys::Hostname;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_logfile;
my $g_workdir;
my $g_wait_on_failover_error;
my $_status_handler;
my $_server_manager;
my $start_datetime;
my $log;
my $error_code;


sub init_config() {
  $log = MHA::ManagerUtil::init_log($g_logfile);

  my ( $sc_ref, $binlog_ref, $disabled_ref ) = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  )->read_config();
  my @servers_config        = @$sc_ref;
  my @binlog_servers_config = @$binlog_ref;
  my @disabled_servers = @$disabled_ref;

  if ( !$g_logfile
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
  return (\@servers_config, \@disabled_servers);
}


sub check_settings($$) {
  my $servers_config_ref = shift;
  my $disabled_config_ref = shift;
  my @servers_config     = @$servers_config_ref;
  my @disabled_config     = @$disabled_config_ref;
  MHA::ManagerUtil::check_node_version($log);
  $_status_handler =
    new MHA::FileStatus( conffile => $g_config_file, dir => $g_workdir );
  $_status_handler->init();
  my $appname = $_status_handler->{basename};

  $_server_manager = new MHA::ServerManager( servers => \@servers_config );
  $_server_manager->set_logger($log);
    $_server_manager->connect_all_and_read_server_status();

  my @dead_servers  = $_server_manager->get_dead_servers();
  my @alive_servers = $_server_manager->get_alive_servers();
  my @alive_slaves  = $_server_manager->get_alive_slaves();
  my $master        = $_server_manager->validate_current_master();


  $_server_manager->print_servers_ascii( $master );

  $log->info("Disabled Servers:");
  foreach( @disabled_config ){
      my $hostinfo = $_->get_hostinfo();
      $log->info("$hostinfo\n");
  }


  $log->info("Dead Servers:");
  $_server_manager->print_dead_servers();
  $log->info("Alive Servers:");
  $_server_manager->print_alive_servers();
  $log->info("Alive Slaves:");
  $_server_manager->print_alive_slaves();
  $log->info("Failed Slaves:");
  $_server_manager->print_failed_slaves_if();
  $log->info("Unmanaged Slaves:");
  $_server_manager->print_unmanaged_slaves_if();

  return 0;
}

sub show_servers_status {
  my $error_code = 1;

  eval {
    my ( $servers_ref, $disabled_ref ) = init_config();
    my @servers_config = @$servers_ref;
    my @disabled_config = @$disabled_ref;
    $log->info("Starting Status Checking.");
    check_settings( \@servers_config, \@disabled_config );
    $_server_manager->disconnect_all() if ($_server_manager);
  };
  if ($@) {
    $log->info($@);
    $_server_manager->disconnect_all() if ($_server_manager);
    undef $@;
  }else{
      $error_code = 0;
  }

  return $error_code;
}

sub finalize_on_error {
  eval {

    # Failover failure happened
    $_status_handler->update_status($MHA::ManagerConst::ST_FAILOVER_ERROR_S)
      if ($_status_handler);
    if ( $g_wait_on_failover_error > 0 ) {
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
  my $a = GetOptions(
    'global_conf=s'            => \$g_global_config_file,
    'conf=s'                   => \$g_config_file,
    'workdir=s'                => \$g_workdir,
    'manager_workdir=s'        => \$g_workdir,
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

  unless ($g_logfile){
      $g_logfile = undef ;
  }
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;
  eval { $error_code = show_servers_status(); };
  if ($@) {
    $error_code = 1;
  }
  if ($error_code) {
    finalize_on_error();
  }
  return $error_code;
}

1;

