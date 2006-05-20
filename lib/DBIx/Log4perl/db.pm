# $Id: db.pm 169 2006-04-24 14:00:13Z martin $
use strict;
use warnings;
use DBI;
use Log::Log4perl;

package DBIx::Log4perl::db;
@DBIx::Log4perl::db::ISA = qw(DBI::db DBIx::Log4perl);
use DBIx::Log4perl::Constants qw (:masks $LogMask);

sub prepare {
  my($dbh, @args) = @_;
  my $h = $dbh->{private_DBIx_Log4perl};
  $dbh->_dbix_l4p_debug('prepare', $args[0])
    if (($LogMask & DBIX_L4P_LOG_INPUT) &&
	  (caller !~ /^DBIx::Log4perl/)); # e.g. from selectall_arrayref
    my $sth = $dbh->SUPER::prepare(@args);
  $sth->{private_DBIx_Log4perl} = $h;
  return $sth;
}

sub do {
  my ($dbh, @args) = @_;
  my $h = $dbh->{private_DBIx_Log4perl};

  $h->{Statement} = $args[0];
  $dbh->_dbix_l4p_debug('do', @args)
    if ($LogMask & DBIX_L4P_LOG_INPUT);

  my $affected = $dbh->SUPER::do(@args);

  if (!$affected &&
	($LogMask & DBIX_L4P_LOG_ERRCAPTURE) &&
	  !($LogMask & DBIX_L4P_LOG_INPUT)) { # not already logged
      $dbh->_dbix_l4p_error('do error for ', @args);
  } elsif (defined($affected) && $affected eq '0E0' &&
	  ($LogMask & DBIX_L4P_LOG_WARNINGS)) {
      $dbh->_dbix_l4p_warning('no effect from ', @args);
  } elsif (($affected ne '0E0') && ($LogMask & DBIX_L4P_LOG_INPUT)) {
      $dbh->_dbix_l4p_debug('affected', $affected);
      $h->{logger}->debug("\t" . $dbh->SUPER::errstr)
	if (!defined($affected));
  }
  return $affected;
}

sub selectrow_array {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $dbh->_dbix_l4p_debug('selectrow_array', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    if (wantarray) {
	my @ret = $dbh->SUPER::selectrow_array(@args);
	$dbh->_dbix_l4p_debug('result', @ret)
	  if ($LogMask & DBIX_L4P_LOG_OUTPUT);
	return @ret;

    } else {
	my $ret = $dbh->SUPER::selectrow_array(@args);
	$dbh->_dbix_l4p_debug('result', $ret)
	  if ($LogMask & DBIX_L4P_LOG_OUTPUT);
	return $ret;
    }
}

sub selectrow_arrayref {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $dbh->_dbix_l4p_debug('selectrow_arrayref', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ref = $dbh->SUPER::selectrow_arrayref(@args);
    $dbh->_dbix_l4p_debug('result', $ref)
      if ($LogMask & DBIX_L4P_LOG_OUTPUT);
    return $ref;
}

sub selectrow_hashref {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $dbh->_dbix_l4p_debug('selectrow_hashref', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ref = $dbh->SUPER::selectrow_hashref(@args);
    # no need to show result - fetch will do this
    return $ref;

}

sub selectall_arrayref {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $dbh->_dbix_l4p_debug('selectall_arrayref', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ref = $dbh->SUPER::selectall_arrayref(@args);
    $dbh->_dbix_l4p_debug('result', $ref)
      if ($LogMask & DBIX_L4P_LOG_OUTPUT);
    return $ref;
}

sub selectall_hashref {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $dbh->_dbix_l4p_debug('selectall_hashref', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ref = $dbh->SUPER::selectall_hashref(@args);
    # no need to show result - fetch will do this
    return $ref;

}
sub disconnect {
    my $dbh = shift;

    my $h = $dbh->{private_DBIx_Log4perl};
    $h->{logger}->debug("disconnect")
      if ($LogMask & DBIX_L4P_LOG_CONNECT);

    return $dbh->SUPER::disconnect;
    
}

sub begin_work {
    my $dbh = shift;
    my $h = $dbh->{private_DBIx_Log4perl};
    $h->{logger}->debug("start transaction")
      if ($LogMask & DBIX_L4P_LOG_TXN);

    return $dbh->SUPER::begin_work;
}

sub rollback {
    my $dbh = shift;
    my $h = $dbh->{private_DBIx_Log4perl};
    $h->{logger}->debug("roll back")
      if ($LogMask & DBIX_L4P_LOG_TXN);

    return $dbh->SUPER::rollback;
}
  
sub commit {
    my $dbh = shift;

    my $h = $dbh->{private_DBIx_Log4perl};
    $h->{logger}->debug("commit")
      if ($LogMask & DBIX_L4P_LOG_TXN);

    return $dbh->SUPER::commit;
}

sub last_insert_id {
    my ($dbh, @args) = @_;

    my $h = $dbh->{private_DBIx_Log4perl};
    $h->{logger}->debug(
	sub {Data::Dumper->Dump([\@args], ['last_insert_id'])})
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ret = $dbh->SUPER::last_insert_id(@args);
    $h->{logger}->debug(sub {"\t" . DBI::neat($ret)})
      if ($LogMask & DBIX_L4P_LOG_INPUT);
    return $ret;
}
1;
