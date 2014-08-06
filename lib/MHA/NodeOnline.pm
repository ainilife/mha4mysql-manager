#!/usr/bin/env perl


package MHA::NodeOnline;

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
my $g_interactive = 1;
my $_server_manager;
my $start_datetime;
my %_node_arg;
my $log;
my $mail_subject;
my $mail_body;
my $_status_handler;
my $_failover_complete_file;
my $_failover_error_file;
my $_create_error_file = 0;

sub init_config() {
  $log = MHA::ManagerUtil::init_log($g_logfile);

  my ( $sc_ref, $binlog_ref ) = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  )->read_config_with_disabled($_node_arg{ip},$_node_arg{port});
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
  my $node;
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
  my $current_master;
  if($g_interactive){
    $_server_manager->connect_all_and_read_server_status();
    $current_master=$_server_manager->get_current_alive_master();
  }else{
    $_server_manager->connect_all_and_read_server_status();
    $current_master=$_server_manager->get_current_alive_master();
  }

  my @dead_servers  = $_server_manager->get_dead_servers();
  my @alive_servers = $_server_manager->get_alive_servers();
  my @alive_slaves  = $_server_manager->get_alive_slaves();
  my @failed_slaves = $_server_manager->get_failed_slaves();

  $log->info(" ok.");

  $log->info("Alive Servers:");
  $_server_manager->print_alive_servers();
  $log->info("Alive Slaves:");
  $_server_manager->print_alive_slaves();
  $log->info("Failed Slaves:");
  $_server_manager->print_failed_slaves_if();
  $log->info("Unmanaged Slaves:");
  $_server_manager->print_unmanaged_slaves_if();

  foreach my $d (@servers_config) {
    if ( $d->{hostname} eq $_node_arg{hostname}
        && $d->{port} eq $_node_arg{port} && $d->{disabled} eq 1) 
    {
        $log->error(
          "The Node $_node_arg{ip}:$_node_arg{port} is already online. Stop mark-up.");
        croak;
    }
  }
  my $node_alive = 0;
  foreach my $d (@alive_servers) {
    if ( $d->{hostname} eq $_node_arg{hostname}
        && $d->{port} eq $_node_arg{port}) 
    {
      $node_alive = 1;
      $node       = $d;
      last;
    }
  }
  unless ($node_alive eq '1') {
    my $node_dead = 0;
    foreach my $d (@dead_servers){
        if ( $d->{hostname} eq $_node_arg{hostname}
            && $d->{port} eq $_node_arg{port}) 
        {
          $node_dead = 1;
          last;
        }
    }
    if( $node_dead eq '1' ){
        $log->error(
          "The slave $_node_arg{ip}:$_node_arg{port} is dead. Please make sure server is alive before mark-up. Stop mark-up.");
        croak;
    }else{
        $log->error(
          "The slave $_node_arg{ip}:$_node_arg{port} is not configured. Please check your configuration file. Stop mark-up.");
        croak;
    }
  }
   
  my $repl_ok = $node->is_repl_ok();
  unless( $repl_ok eq '1' ){
    $log->error(
      "Replication Status of Node $node->{hostname}:$node->{port} is not OK. Stop mark-up");
    croak;
  }

  my $node2online_master_ip   = $node->{Master_Host};
  my $node2online_master_port = $node->{Master_Port};
  my ($curent_master_ip,$curent_master_port)
    = ($current_master->{hostname},$current_master->{port});

  unless ( $node2online_master_ip eq $curent_master_ip
      && $node2online_master_port eq $curent_master_port ){
    $log->error(
      "Current Master is $curent_master_ip:$curent_master_port, but the node to-be mark-up is the slave of $node2online_master_ip:$node2online_master_port. Stop mark-up.");
    croak;
  }



  if ($g_interactive) {
    print "Slave $node->{hostname}:$node->{port} is alive. Proceed? (yes/NO): ";
    my $ret = <STDIN>;
    chomp($ret);
    die "Stopping mark-up." if ( lc($ret) eq 'yes' );
  }

  $_server_manager->get_failover_advisory_locks();

  return $node;
}

sub mark_up_node_internal($) {
  my $node = shift;

  $log->info(
"Calling mark-up online script before we mark-up online MHA."
  );

  if ( $node->{node_ip_online_script} ) {
    my $command =
"$node->{node_ip_online_script} --new_node_host=$node->{hostname} --new_node_ip=$node->{ip} --new_node_port=$node->{port}";
  $command .= " --command=online";
    $log->info("Executing node mark-up script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" done.");
      $mail_body .=
        "Notify external with script for node mark-up $node->{hostname} succeeded .\n";
    }
    else {
      my $message =
        "Failed to notify external with script for node mark-up, return code $high:$low";
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
"node_ip_online_script is not set. Skipping notify external systems for mark-up."
    );
  }

  return 0;
}

sub mark_up_node($) {
  my $node = shift;

  my $appname      = $_status_handler->{basename};
  my @alive_slaves = $_server_manager->get_alive_slaves();
  $mail_subject = $appname . ": MySQL Node mark-up $node->{hostname}";
  $mail_body    = "Slave $node->{hostname} is Marking-UP now!\n\n";

  $mail_body .= "Check MHA Manager logs at " . hostname();
  $mail_body .= ":$g_logfile" if ($g_logfile);
  $mail_body .= " for details.\n\n";
  if ($g_interactive) {
    $mail_body .= "Started manual(interactive) mark-up.\n";
  }
  else {
    $mail_body .= "Started automated(non-interactive) mark-up.\n";
  }

  mark_up_node_internal($node);

  return 0;
}

sub cleanup {
  $_server_manager->release_failover_advisory_lock();
  $_server_manager->disconnect_all();
  MHA::NodeUtil::create_file_if($_failover_complete_file);
  $_create_error_file = 0;
  return 0;
}

sub do_node_switch_online() {
  my $error_code = 1;
  my $node;

  eval {
    my @servers_config = init_config();
    $log->info("Starting Node Online Procedure\n");
    $log->info();
    $log->info("* Phase 1: Configuration Check Phase..\n");
    $log->info();
    $node = check_settings( \@servers_config );

    $log->info("** Phase 1: Configuration Check Phase completed.\n");
    $log->info();
    $log->info("* Phase 2: Enable Node Phase..\n");
    $log->info();
    $error_code = mark_up_node($node);
    $log->info("* Phase 2: Enable Node Phase completed.\n");

    if ( $error_code == 0 ) {
      MHA::Config::enable_block_and_save( $g_config_file, $node->{id},
        $log );
    }
    cleanup();
  };
  if ($@) {
    if ( $node && $node->{not_error} ) {
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

sub main {
  local $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = \&exit_by_signal;
  local @ARGV = @_;
  my ( $node_host, $node_ip, $node_port, $error_code );
  my $a = GetOptions(
    'global_conf=s'            => \$g_global_config_file,
    'conf=s'                   => \$g_config_file,
    'workdir=s'                => \$g_workdir,
    'manager_workdir=s'        => \$g_workdir,
    'interactive=i'            => \$g_interactive,
    'new_node_host=s'       => \$node_host,
    'new_node_ip=s'         => \$node_ip,
    'new_node_port=i'       => \$node_port,
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
  unless ($node_host) {
    print "--new_node_host=<online_node_host> must be set.\n";
    return 1;
  }
  unless ($node_ip) {
    $node_ip = MHA::NodeUtil::get_ip($node_host);
    print "--new_node_ip=<online_node_ip> is not set. Using $node_ip.\n";
  }
  unless ($node_port) {
    $node_port= 3306;
    print "--new_node_port=<online_node_port> is not set. Using $node_port.\n";
  }

  $_node_arg{hostname} = $node_host;
  $_node_arg{ip}       = $node_ip;
  $_node_arg{port}     = $node_port;
  $g_logfile = undef if ($g_interactive);
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;
  eval { $error_code = do_node_switch_online(); };
  if ($@) {
    $error_code = 1;
  }
  if ($error_code) {
      #finalize_on_error();
  }
  return $error_code;
}

1;
