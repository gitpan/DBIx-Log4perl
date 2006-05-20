# $Id: Log4perl.pm 184 2006-05-20 17:39:23Z martin $
require 5.008;

use strict;
use warnings;
use Carp;
use Log::Log4perl;
use Data::Dumper;

package DBIx::Log4perl;
use DBIx::Log4perl::Constants qw (:masks $LogMask);
use DBIx::Log4perl::db;
use DBIx::Log4perl::st;

our $VERSION = '0.03';
require Exporter;
our @ISA = qw(Exporter DBI);		# look in DBI for anything we don't do

our @EXPORT = ();		# export nothing by default
our @EXPORT_MASKS = qw(DBIX_L4P_LOG_DEFAULT
		       DBIX_L4P_LOG_ALL
		       DBIX_L4P_LOG_INPUT
		       DBIX_L4P_LOG_OUTPUT
		       DBIX_L4P_LOG_CONNECT
		       DBIX_L4P_LOG_TXN
		       DBIX_L4P_LOG_ERRCAPTURE
		       DBIX_L4P_LOG_WARNINGS
		     );
our %EXPORT_TAGS= (masks => \@EXPORT_MASKS);
Exporter::export_ok_tags('masks'); # all tags must be in EXPORT_OK


my $glogger;

sub _dbix_l4p_debug {
    my ($self, $thing, @args) = @_;

    my $h = $self->{private_DBIx_Log4perl};
    $Data::Dumper::Indent = 0;

    if (scalar(@args) > 1) {
	$h->{logger}->debug(
	    sub {Data::Dumper->Dump([\@args], [$thing])})
    } elsif (ref($args[0])) {
	$h->{logger}->debug(
	    sub {Data::Dumper->Dump([$args[0]], [$thing])})
    } elsif (scalar(@args) == 1) {
	if (!defined($args[0])) {
	    $h->{logger}->debug("$thing:");
	} else {
	    $h->{logger}->debug("$thing: " . DBI::neat($args[0]));
	}
    }
}
sub _dbix_l4p_warning {
    my ($self, $thing, @args) = @_;

    my $h = $self->{private_DBIx_Log4perl};
    $Data::Dumper::Indent = 0;

    if (scalar(@args) > 1) {
	$h->{logger}->warn(
	    sub {Data::Dumper->Dump([\@args], [$thing])})
    } elsif (ref($args[0])) {
	$h->{logger}->warn(
	    sub {Data::Dumper->Dump([$args[0]], [$thing])})
    } else {
	$h->{logger}->warn("$thing: " . DBI::neat($args[0]));
    }
}
sub _dbix_l4p_error {
    my ($self, $thing, @args) = @_;

    my $h = $self->{private_DBIx_Log4perl};
    $Data::Dumper::Indent = 0;

    if (scalar(@args) > 1) {
	$h->{logger}->error(
	    sub {Data::Dumper->Dump([\@args], [$thing])})
    } elsif (ref($args[0])) {
	$h->{logger}->error(
	    sub {Data::Dumper->Dump([$args[0]], [$thing])})
    } else {
	$h->{logger}->error("$thing: " . DBI::neat($args[0]));
    }
}

sub _dbix_l4p_attr_map {
    return {DBIx_l4p_logger => 'logger',
	    DBIx_l4p_init => 'init',
	    DBIx_l4p_class => 'class'
	   };
}

sub dbix_l4p_getattr {
    my ($self, $item) = @_;

    die "wrong arguments - dbix_l4p_getattr(attribute_name)"
      if (scalar(@_) != 2 || !defined($_[1]));

    my $m = _dbix_l4p_attr_map();

    my $h = $self->{private_DBIx_Log4perl};

    if (!exists($m->{$item})) {
	warn "$item does not exist";
	return undef;
    }
    return $h->{$m->{$item}};
}

sub dbix_l4p_setattr {
    my ($self, $item, $value) = @_;

    die "wrong arguments - dbix_l4p_setattr(attribute_name, value)"
      if (scalar(@_) != 3 || !defined($_[1]));

    my $m = _dbix_l4p_attr_map();

    my $h = $self->{private_DBIx_Log4perl};

    if (!exists($m->{$item})) {
	warn "$item does not exist";
	return undef;
    }
    $h->{$m->{$item}} = $value;
    1;
}
#
# Error handler to capture errors and log them
# Whatever, errors are passed on.
# if the user of DBIx::Log4perl passed in an error handler that is called
# before exiting.
#
sub _error_handler {
    my ($msg, $handle, $method_ret) = @_;

    my $dbh = $handle;
    my $lh;
    my $h = $handle->{private_DBIx_Log4perl};
    my $out = '';

    $lh = $glogger;
    $lh = $h->{logger} if ($h && exists($h->{logger}));
    return 0 if (!$lh);

    $out .=  "  " . "=" x 60 . "\n  $msg\n";

    $out .= '  lasth type: ' . $DBI::lasth->{Type} . "\n"
      if ($DBI::lasth->{Type});
    $out .= '  last Statement: ' . $DBI::lasth->{Statement}
      if ($DBI::lasth->{Statement});
    # get db handle if we have an st
    my $type = $handle->{Type};
    my (@pos_sts, $sql);
    if ($type eq "st") {
	$dbh = $handle->{Database};
	$sql = $handle->{Statement};

    } else {
	# get list of statements under the db
	push @pos_sts, $_ for (grep { defined } @{$h->{ChildHandles}});
	$out .= "  " . $#pos_sts . " sub statements\n";
	# We have a db and want the sql
	# We've got other sts under this db but we cannot tell if ANY of
	# them are the right one (or possibly none - e.g. app using do())
	$sql = 'Possible SQL: ';
	$sql .= '/' . $h->{Statement} . "/\n  " if (exists($h->{Statement}));
	$sql .= '/' . $dbh->{Statement} . "/\n  "
	  if ($dbh->{Statement} &&
		(exists($h->{Statement}) && 
			  ($dbh->{Statement} ne $h->{Statement})));
	foreach my $stmt (@pos_sts) {
	    $sql .= '/' . $stmt->{Statement} . "/\n  "
	      if ($stmt->{Statement} &&
		    (exists($h->{Statement}) &&
			      ($h->{Statement} ne $stmt->{Statement})));
	}
    }

    my $dbname = exists($dbh->{Name}) ? $dbh->{Name} : "";
    my $username = exists($dbh->{Username}) ? $dbh->{Username} : "";
    $out .= "  DB: $dbname, Username: $username\n";

    $out .= "  handle type: $type\n  SQL: $sql\n";
    $out .= "  msg: " . $handle->errstr . "\n" if ($handle->errstr);
    if ($type eq 'st') {
	my $str = "";
	if ($handle->{ParamValues}) {
	    map {$str .= $handle->{ParamValues}->{$_} . ","}
	      sort keys %{$handle->{ParamValues}};
	}
	$out .= "  ParamValues: $str\n";
	$out .= "  " .
	  Data::Dumper->Dump([$handle->{ParamArrays}], ['ParamArrays'])
	      if ($handle->{ParamArrays});
    }
    local $Carp::MaxArgLen = 256;
    $out .= "  " .Carp::longmess("DBI error trap");
    $out .= "  " . "=" x 60 . "\n";
    $lh->fatal($out);
    if ($h && exists($h->{ErrorHandler})) {
      return $h->{ErrorHandler}($msg, $handle, $method_ret);
    } else {
      return 0;			# pass error on
    }
}

sub connect {
    my ($drh, $dsn, $user, $pass, $attr) = @_;
    my %h = ();
    my $log;
    if ($attr) {
      # check we have not got DBIx_l4p_init without DBIx_l4p_log or vice versa
	my ($a, $b) = (exists($attr->{DBIx_l4p_init}),
		       exists($attr->{DBIx_l4p_class}));
	die "DBIx_l4p_init specified without DBIx_l4p_class or vice versa"
	  if (($a xor $b));
	# if passed a Log4perl log handle use that
	if (exists($attr->{DBIx_l4p_logger})) {
	    $h{logger} = $attr->{DBIx_l4p_logger};
	} elsif ($a && $b) {
	    Log::Log4perl->init($attr->{DBIx_l4p_init});
	    $h{logger} = Log::Log4perl->get_logger($attr->{DBIx_l4p_class});
	    $h{init} = $attr->{DBIx_l4p_init};
	    $h{class} = $attr->{DBIx_l4p_class};
	} else {
	    $h{logger} = Log::Log4perl->get_logger(); # "DBIx::Log4perl"
	}
	# save log mask
	$LogMask = $attr->{DBIx_l4p_logmask}
	  if (exists($attr->{DBIx_l4p_logmask}));
	if ($LogMask & DBIX_L4P_LOG_ERRCAPTURE) {
	    # save any error handler for DBI and replace with our own
	    $h{HandleError} = $attr->{HandleError}
	      if (exists($attr->{HandleError}));
	    $attr->{HandleError} = \&_error_handler;
	}
	# remove our attrs from connection attrs
	delete $attr->{DBIx_l4p_init};
	delete $attr->{DBIx_l4p_class};
	delete $attr->{DBIx_l4p_logger};
	delete $attr->{DBIx_l4p_logmask};
    }
    $h{logger} = Log::Log4perl->get_logger() if (!exists($h{logger}));
    $glogger = $h{logger};
    my $dbh = $drh->SUPER::connect($dsn, $user, $pass, $attr);
    return $dbh if (!$dbh);

    $dbh->{private_DBIx_Log4perl} = \%h;
    if ($LogMask & DBIX_L4P_LOG_CONNECT) {
	$h{logger}->debug("connect: $dsn, $user");
	no strict 'refs';
	my $v = "DBD::" . $dbh->{Driver}->{Name} . "::VERSION";
	$h{logger}->info("DBI: " . $DBI::VERSION,
			 ", DBIx::Log4perl: " . $DBIx::Log4perl::VERSION .
			   ", Driver: " . $dbh->{Driver}->{Name} . "(" .
			     $$v . ")");
    }
    return $dbh;
}

sub dbix_l4p_logdie
{
    my ($drh, $msg) = @_;
    my ($dbh, $sth);

    _error_handler($msg, $drh);
    die "$msg";
}

1;

__END__

=head1 NAME

DBIx::Log4perl - Perl extension for DBI to selectively log SQL,
parameters, result-sets, transactions etc to a Log::Log4perl handle.

=head1 SYNOPSIS

  use Log::Log4perl;
  use DBIx::Log4perl;

  Log::Log4perl->init("/etc/mylog.conf");
  my $dbh - DBIX::Log4perl->connect('DBI:odbc:mydsn', $user, $pass);
  $dbh->DBI_METHOD(args);

  or

  use DBIx::Log4perl;
  my $dbh - DBIX::Log4perl->connect('DBI:odbc:mydsn', $user, $pass
                                    {DBIx_l4p_log => /etc/mylog.conf,
                                     DBIx_l4p_class => "My::Package");
  $dbh->DBI_METHOD(args);

=head1 DESCRIPTION

C<DBIx::Log4perl> is a wrapper over DBI which adds logging of your DBI
activity via a Log::Log4perl handle. Log::Log4perl has many advantages
for logging but the ones probably most attractive are:

The ability to turn logging on or off or change the logging you see
without changing your code.

Different log levels allowing you to separate warnings, errors and fatals
to different files.

=head1 METHODS

DBIx::Log4perl adds the following methods over DBI.

=head2 dbix_l4p_getattr

  $h->dbxi_l4p_getattr('DBIx_l4p_logmask');

Returns the value for a DBIx::Log4perl attribute (see L</Attributes>).

=head2 dbix_l4p_setattr

 $h->dbix_l4p_setattr('DBIx_l4p_logmask', 1);

Seta the value of the specified DBIx::Log4perl attribute
(see L</Attributes>).

=head2 dbix_l4p_logdie

  $h->dbix_l4p_logdie($message);

Calls the internal _error_handler method with the message $message
then dies with Carp::confess.

The internal error handler is inserted into DBI's HandleError if
DBIX_L4P_LOG_ERRCAPTURE is enabled. It attempts to log as much
information about the SQL you were executing, parameters etc.

As an example, you might be checking a $dbh->do which attempts to
update a row really does update a row and want to die with all possible
information about the problem if the update fails. Failing to update a
row would not ordinarily cause DBI's error handler to be called.

  $affected = $dbh->do(q/update table set column = 1 where column = 2/);
  $dbh->dbix_logdie("Update failed") if ($affected != 1);

=head1 GLOBAL VARIABLES

=head2 DBIx::Log4perl::LogMask

This variable controls the amount of logging logged to the
Log::Log4perl handle. There are a number of constants defined which
may be ORed together to obtain the logging level you require:

=over

=item DBIX_L4P_LOG_DEFAULT

By default LogMask is set to DBIX_L4P_LOG_DEFAULT which is currently
DBIX_L4P_LOG_TXN | DBIC_L4P_LOG_CONNECT | DBIX_L4P_LOG_INPUT | DBIX_L4P_LOG_ERRCAPTURE.

=item DBIX_L4P_LOG_ALL

Log everything, all possible masks ORed together.

=item DBIX_L4P_LOG_INPUT

Log at Log4perl debug level input SQL to C<do>, C<prepare>, select*
methods and any value returned from C<last_insert_id>. In addition, if
the SQL is an insert/update/delete statement the rows afffected will
be logged.

NOTE: Many databases return 0 rows affected for DDL statements like
create, drop etc.

=item DBIX_L4P_LOG_OUTPUT

Log at Log4perl debug level the result-sets generated by select* or
fetch* methods. Be careful, this could produce a lot of output if you
produce large result-sets.

=item DBIX_L4P_LOG_CONNECT

Log at Log4perl debug level any call to the C<connect> and
C<disconnect> methods and their arguments.

On connect the DBI version, DBIx::Log4perl version, the driver name
and version will be logged at Log4perl info level.

=item DBIX_L4P_LOG_TXN

Log at Log4perl debug level all calls to C<begin_work>, C<commit> and
C<rollback>.

=item DBIX_L4P_LOG_WARNINGS

Log at Log4perl warning level any calls to do will return no affected
rows on an insert, update or delete opertion.

=item DBIX_L4P_LOG_ERRCAPTURE

Install a DBI error handler which logs at Log4perl fatal level
as much information as it can about any trapped error. This includes
some or all of the following depending on what is available:

  Handle type being used
  Number of statements under the current connection
  Name of database
  Username for connection to database
  Any SQL being executed at the time
  The error message text
  Any parameters in ParamValues
  Any parameters in ParamArrays
  A stack trace of the error

If you install your own error handler in the C<connect> call it will
be replaced when C<connect> is called in DBI but run from
DBIx::Log4perl's error handler.

C<DBIx::Log4perl> always returns 0 from the error handler if it is the
only handler which causes the error to be passed on. If you have
defined your own error handler then whatever your handler returns is
passed on.


=back


=head1 ATTRIBUTES

When you call connect you may add C<DBIx::Log4perl> attributes to those
which you are passing to DBI. You may also get and set attributes after
connect using C<dbix_l4p_getattr()> and C<dbix_l4p_setattr()>.
C<DBIx::Log4perl> supports the following attributes:

=over

=item C<DBIx_l4p_init>

This is the string to pass on to Log::Log4Perl's init method. It is
the name of the Log::Log4perl configuration file to use. e.g.

  Log::Log4perl::init('/etc/log4perl.conf');

See Log::Log4perl.

=item C<DBIx_l4p_log>

This is the string to pass on to Log::Log4Perl's get_logger method
e.g.

  $logger = Log::Log4perl->get_logger('mysys.dbi');

See Log::Log4perl.

=item C<DBIx_l4p_logger>

If you have already initialised and created your own Log::Log4perl
handle you can pass it in as DBIx_l4p_logger and C<DBIx::Log4perl>
will ignore DBIx_l4p_log and DBIx_l4p_init.

=back

Although these attributes are supported the recommended way to use
DBIx::Log4perl it to use Log::Log4perl in your application and call
the Log::Log4Perl->init to define your log4perl configuration file.
DBIx::Log4perl will then call
Log::Log4perl->get_logger("DBIx::Log4perl") (as was intended by the
authors of Log::Log4perl) and all you need is a
C<log4perl.logger.DBIx.Log4perl> entry in your configuration file.

=head1 Log::Log4perl CONFIGURATION FILE

Please see Log::Log4perl for full details of the configuration file
and appenders. DBIx::Log4perl contains a sample configuration file you
may use to get started. It looks like this:

  log4perl.logger = FATAL, LOGFILE
      
  log4perl.appender.LOGFILE=Log::Log4perl::Appender::File
  log4perl.appender.LOGFILE.filename=/tmp/log
  log4perl.appender.LOGFILE.mode=append
  log4perl.appender.LOGFILE.Threshold = ERROR
      
  log4perl.appender.LOGFILE.layout=PatternLayout
  log4perl.appender.LOGFILE.layout.ConversionPattern=[%r] %F %L %c - %m%n
  
  log4perl.logger.DBIx.Log4perl=DEBUG, A1
  log4perl.appender.A1=Log::Log4perl::Appender::File
  log4perl.appender.A1.filename=/tmp/xlog
  log4perl.appender.A1.mode=append
  log4perl.appender.A1.layout=Log::Log4perl::Layout::SimpleLayout

This is perhaps the most simple configuration. It says fatal errors go
to /tmp/log and debug and above go to /tmp/xlog. It also uses the
SimpleLayout which prefixes each line with the log level. You can
use:
 
  log4perl.appender.A1.layout=Log::Log4perl::Layout::PatternLayout
  log4perl.appender.A1.layout.ConversionPattern=%d %p> %F{1}:%L %M - %m%n

to make Log4perl prefix the line with a timestamp, module name and
filename but the latter two are not too useful since they will always
report a position in DBIx::Log4perl instead of your module. I'd
welcome any suggestion to get around this.

=head1 FORMAT OF LOG

=head2 Example output

For a connect the log will contain something like:

  DEBUG - connect: DBI:mysql:mjetest, bet
  INFO - DBI: 1.50, DBIx::Log4perl: 0.01, Driver: mysql(3.0002_4)

For

  $sth = $dbh->prepare('insert into mytest values (?,?)');
  $sth->execute(1, 'one');

you will get:

  DEBUG - prepare: 'insert into mytest values (?,?)'
  DEBUG - $execute = [1,'one'];

NOTE: Some DBI methods are combinations of various methods
e.g. selectrow_* methods. For some of these methods DBI does not
actually call all the lower methods because the driver implements
selectrow_* methods in C. For these cases, DBIx::Log4perl will only be
able to log the selectrow_* method, the SQL, any parameters and any
returned result-set and you will not necessarily see a prepare,
execute and fetch in the log. e.g.:

  $dbh->selectrow_array('select b from mytest where a = ?',undef,1);

results in:

  DEBUG - $selectrow_array = ['select b from mytest where a = ?',undef,1];

with no evidence prepare/execute/fetch was called.

If DBIX_L4P_LOG_ERRCAPTURE is set all possible information about an
error is written to the log by the error handler. In addition a few
method calls will attempt to write a separate log entry containing
information which may not be available in the error handler e.g.

  $sth = $dbh->prepare(q/insert into mytest values (?,?)/);
  $sth->bind_param_array(1, [51,1,52,53]);
  $sth->bind_param_array(2, ['fiftyone', 'one', 'fiftythree', 'fiftytwo']);
  $inserted = $sth->execute_array( { ArrayTupleStatus => \@tuple_status } );

when the mytest table has a primary key on the first column and a row
with 1 already exists will result in:

  ERROR - $Error = [1062,'Duplicate entry \'1\' for key 1','S1000'];
  ERROR -          for 1,fiftytwo

because the @tuple_status is not available in the error handler. In
this output 1062 is the native database error number, the second
argument is the error text, the third argument the state and the
additional lines attempt to highlight the parameters which caused the
problem.

=head2 Use of Data::Dumper

DBIX::log4perl makes extensive use of Data::Dumper to output arguments
passed to DBI methods. In some cases it combines the method called
with the data it is logging e.g.

  DEBUG - $execute = [2,'two'];

This means the execute method was called with placeholder arguments
of 2 and 'two'. The '$' prefixing execute is because Data::Dumper was
called like this:

  Data::Dumper->dump( [ \@execute_args ], [ 'execute'] )

so Data::Dumper believe it is dumping $execute. DBIx::Log4perl uses
this method extensively to log the method and arguments - just ignore
the leading '$' in the log.


=head1 NOTES

During the development of this module I came across of large number of
issues in DBI and various DBDs. I've tried to list them here but in
some cases I cannot give the version the problem was fixed in because
it was not released at the time of writing.

=head2 DBI and $h->{Username}

If you get an error like:

  Can't get DBI::dr=HASH(0x83cbbc4)->{Username}: unrecognised attribute name

in the error handler it is because it was missing from DBI's XS code.

This is probably fixed in DBI 1.51.

The actual code you need to change is:

  --- DBI.xs.orig Mon Mar 27 16:37:29 2006
  +++ DBI.xs      Mon Mar 27 16:38:10 2006
  @@ -1947,6 +1947,7 @@
          else if (!(     (*key=='H' && strEQ(key, "HandleError"))
                  ||      (*key=='H' && strEQ(key, "HandleSetErr"))
                  ||      (*key=='S' && strEQ(key, "Statement"))
  +               ||      (*key=='U' && strEQ(key, "Username"))
                  ||      (*key=='P' && strEQ(key, "ParamValues"))
                  ||      (*key=='P' && strEQ(key, "Profile"))
                  ||      (*key=='C' && strEQ(key, "CursorName"))

=head2 DBI and $h->{ParamArrays}

This is the same issue as above for $h->{Username}.

=head2 DBD::ODBC and ParamValues

In DBD::ODBC 1.13 you cannot obtain ParamValues after an execute has
failed. I believe this is because DBD::ODBC insists on describing a
result-set before returning ParamValues and that is not necessary for
ParamValues. I've mailed Jeff Urlwin about this and he is looking
into it.

=head2 DBD::mysql and ParamArrays

DBD::mysql 3.002_4 does not support ParamArrays.

I had to add the following to dbdimp.c to make it work:

  case 'P':
    if (strEQ(key, "PRECISION"))
      retsv= ST_FETCH_AV(AV_ATTRIB_PRECISION);
    /* + insert the following block */
    if (strEQ(key, "ParamValues")) {
        HV *pvhv = newHV();
        if (DBIc_NUM_PARAMS(imp_sth)) {
            unsigned int n;
            SV *sv;
            char key[100];
            I32 keylen;
            for (n = 0; n < DBIc_NUM_PARAMS(imp_sth); n++) {
                keylen = sprintf(key, "%d", n);
                hv_store(pvhv, key, keylen, newSVsv(imp_sth->params[n].value), 0);
            }
        }
        retsv = newRV_noinc((SV*)pvhv);
    }
    /* - end of inserted block */
    break;

I believe this code is now added in DBD::mysql 3.0003_1.

=head1 REQUIREMENTS

You will need at least Log::Log4perl 1.04 and DBI 1.50.

DBI-1.51 (I hope) will contain the changes listed under L</NOTES>.

Version of Log::Log4perl before 1.04 work but unfortunately you will
get code references in some of the log output where DBIx::Log4perl
does:

  $log->logwarn(sub {Data::Dumper->Dump(something)})

The same applies to logdie. See the Log4perl mailing list for details.

=head1 SEE ALSO

DBI

Log::Log4perl

=head1 AUTHOR

M. J. Evans, E<lt>mjevans@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by M. J. Evans

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.7 or,
at your option, any later version of Perl 5 you may have available.


=cut
