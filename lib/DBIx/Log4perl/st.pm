# $Id: st.pm 265 2006-08-04 15:51:00Z martin $
use strict;
use warnings;
use DBI;
use Log::Log4perl;

package DBIx::Log4perl::st;
@DBIx::Log4perl::st::ISA = qw(DBI::st DBIx::Log4perl);
use DBIx::Log4perl::Constants qw (:masks $LogMask);

sub finish {
    my ($sth) = shift;
    my $h = $sth->{private_DBIx_Log4perl};
    
    $sth->_dbix_l4p_debug('finish')
	if ($h->{logmask} & DBIX_L4P_LOG_INPUT);
    return $sth->SUPER::finish;
}
	    
sub execute {
    my ($sth, @args) = @_;
    my $h = $sth->{private_DBIx_Log4perl};

    $sth->_dbix_l4p_debug('execute', @args)
      if (($h->{logmask} & DBIX_L4P_LOG_INPUT) && (caller !~ /^DBD::/));

    my $ret = $sth->SUPER::execute(@args);

    if (!$ret) {		# error
	$h->{logger}->error("\tfailed with " . $sth->errstr)
	    if (($h->{logmask} && DBIX_L4P_LOG_ERRCAPTURE) && # logging errors
		(caller !~ /^DBD::/)); # not called from DBD e.g. execute_array
    } elsif (defined($ret)) {
        $sth->_dbix_l4p_debug('affected', $ret)
	    if ((!defined($sth->{NUM_OF_FIELDS})) && # not a result-set
		($h->{logmask} & DBIX_L4P_LOG_INPUT)	&& # logging input
		(caller !~ /^DBD::/));
    }
    return $ret;
}

sub execute_array {
    my ($sth, @args) = @_;
    my $h = $sth->{private_DBIx_Log4perl};

    $sth->_dbix_l4p_debug('execute_array', @args)
      if ($h->{logmask} & DBIX_L4P_LOG_INPUT);

    if (($#args >= 0) && ($args[0]) &&
	    (ref($args[0]) eq 'HASH') &&
		(!exists($args[0]->{ArrayTupleStatus}))) {
	$args[0]->{ArrayTupleStatus} = \my @tuple_status;
    } elsif (!$args[0]) {
	$args[0] = {ArrayTupleStatus => \my @tuple_status};
    }
    my $executed = $sth->SUPER::execute_array(@args);
    if (!$executed) {
        #print Data::Dumper->Dump([$sth->{ParamArrays}], ['ParamArrays']), "\n";
	return $executed unless ($h->{logmask} & DBIX_L4P_LOG_ERRORS);
	return $executed if (!$args[0] || ref($args[0] ne 'HASH') ||
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
		if (ref($pa->{$p})) {
		    push @plist, $pa->{$p}->[$n];
		} else {
		    push @plist, $pa->{$p};
		}
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
    if ($h->{logmask} & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param(@args);
}

sub bind_param_inout {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  $sth->_dbix_l4p_debug('bind_param_inout', @args)
    if ($h->{logmask} & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param_inout(@args);
}

sub bind_param_array {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  $sth->_dbix_l4p_debug('bind_param_array', @args)
    if ($h->{logmask} & DBIX_L4P_LOG_INPUT);
  return $sth->SUPER::bind_param_array(@args);
}

sub fetch {			# alias for fetchrow_arrayref
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetch(@args);
  $h->{logger}->debug(sub {Data::Dumper->Dump([$res], ['fetch'])})
    if ($h->{logmask} & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

sub fetchrow_arrayref {			# alias for fetchrow_arrayref
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetchrow_arrayref(@args);
  $h->{logger}->debug(sub {Data::Dumper->Dump([$res],
						   ['fetchrow_arrayref'])})
    if ($h->{logmask} & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

sub fetchrow_array {
  my ($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my @row = $sth->SUPER::fetchrow_array(@args);
  $h->{logger}->debug(sub {
			     Data::Dumper->Dump([\@row], ['fetchrow_array'])})
    if ($h->{logmask} & DBIX_L4P_LOG_OUTPUT);
  return @row;
}

sub fetchrow_hashref {
  my($sth, @args) = @_;
  my $h = $sth->{private_DBIx_Log4perl};

  my $res = $sth->SUPER::fetchrow_hashref(@args);
  $h->{logger}->debug(
      sub {Data::Dumper->Dump([$res], ['fetchrow_hashref'])})
    if ($h->{logmask} & DBIX_L4P_LOG_OUTPUT);
  return $res;
}

1;
