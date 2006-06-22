# $Id: st.pm 208 2006-06-21 15:10:33Z martin $
use strict;
use warnings;
use DBI;
use Log::Log4perl;

package DBIx::Log4perl::st;
@DBIx::Log4perl::st::ISA = qw(DBI::st DBIx::Log4perl);
use DBIx::Log4perl::Constants qw (:masks $LogMask);

sub execute {
    my ($sth, @args) = @_;
    my $h = $sth->{private_DBIx_Log4perl};

    $sth->_dbix_l4p_debug('execute', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);

    my $ret = $sth->SUPER::execute(@args);

    if ((!$ret) &&		# error
	  ($LogMask && DBIX_L4P_LOG_ERRCAPTURE) && # logging errors
	  caller !~ /^DBD::/) {	# not called from DBD e.g. execute_array
	$sth->_dbix_l4p_error('execute', @args)
	  if (!($LogMask & DBIX_L4P_LOG_INPUT));
	$h->{logger}->error("\tfailed with " . $sth->errstr);
    } elsif (defined($ret) &&
	     (!defined($sth->{NUM_OF_FIELDS})) &&
	     ($LogMask & DBIX_L4P_LOG_INPUT)) {
        $sth->_dbix_l4p_debug('affected', $ret);
    }
    return $ret;
}

sub execute_array {
    my ($sth, @args) = @_;
    my $h = $sth->{private_DBIx_Log4perl};

    $sth->_dbix_l4p_debug('execute_array', @args)
      if ($LogMask & DBIX_L4P_LOG_INPUT);
    my $executed = $sth->SUPER::execute_array(@args);
    if (!$executed) {
        #print Data::Dumper->Dump([$sth->{ParamArrays}], ['ParamArrays']), "\n";
	return $executed if (ref($args[0] ne 'HASH') ||
			       !exists($args[0]->{ArrayTupleStatus}));
        my $pa = $sth->{ParamArrays};
        if (scalar(@args) > 0) {
	    $h->{logger}->error("execute_array error:");
	  my $ats = $args[0]->{ArrayTupleStatus};
	  for my $n (0..@{$ats}-1) {
	    next if (!ref($ats->[$n]));
	    $sth->_dbix_l4p_error('Error', $ats->[$n]);
	    my @plist;
	    foreach my $p (keys %{$pa}) {
	      push @plist, $pa->{$p}->[$n];
	    }
	    $h->{logger}->error(sub {"\t for " . join(',', @plist)});
	  }
	}
    }
    return $executed;
}

sub bind_param {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  $sth->_dbix_l4p_debug('bind_param', @args)
    if ($LogMask & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param(@args);
}

sub bind_param_inout {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  $sth->_dbix_l4p_debug('bind_param_inout', @args)
    if ($LogMask & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param_inout(@args);
}

sub bind_param_array {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  $sth->_dbix_l4p_debug('bind_param_array', @args)
    if ($LogMask & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param_array(@args);
}

sub fetch {			# alias for fetchrow_arrayref
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetch(@args);
  $h->{logger}->debug(sub {Data::Dumper->Dump([$res], ['fetch'])})
    if ($LogMask & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

sub fetchrow_arrayref {			# alias for fetchrow_arrayref
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetchrow_arrayref(@args);
  $h->{logger}->debug(sub {Data::Dumper->Dump([$res],
						   ['fetchrow_arrayref'])})
    if ($LogMask & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

sub fetchrow_array {
  my ($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my @row = $sth->SUPER::fetchrow_array(@args);
  $h->{logger}->debug(sub {
			     Data::Dumper->Dump([\@row], ['fetchrow_array'])})
    if ($LogMask & DBIX_L4P_LOG_OUTPUT);
  return @row;
}

sub fetchrow_hashref {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetchrow_hashref(@args);
  $h->{logger}->debug(
      sub {Data::Dumper->Dump([$res], ['fetchrow_hashref'])})
    if ($LogMask & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

1;
