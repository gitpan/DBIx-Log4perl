use 5.008000;
use ExtUtils::MakeMaker;
use Getopt::Long();
use Data::Dumper;

use vars qw($opt);
$opt = {};

Getopt::Long::GetOptions(
    $opt,
    "help" => sub {Usage()},
    "dsn=s",
    "user=s",
    "password:s",
    "table=s",
    ) || die Usage();

open OUT, ">dbixl4p.config";
print OUT "$opt->{dsn}\n" if (exists($opt->{dsn}));
print OUT "$opt->{user}\n" if (exists($opt->{user}));
if (exists($opt->{password})) {
    print OUT "$opt->{password}\n";
} else {
    print OUT "UNDEF\n";
}
print OUT "$opt->{table}\n" if (exists($opt->{table}));
close OUT;

warn "\nPlease NOTE\nYou need to set and export the DBI_* environment variables or supply suitable values on the command line (see Perl Makefile.PL help) to run a complete test\n\n";

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'DBIx::Log4perl',
    VERSION_FROM      => 'lib/DBIx/Log4perl.pm', # finds $VERSION
    PREREQ_PM         => {Log::Log4perl => 1.04,
		          DBI => 1.50}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/DBIx/Log4perl.pm', # retrieve abstract from module
       AUTHOR         => 'Martin J. Evans <mjevans@cpan.org>') : ()),
);


sub Usage {
  print STDERR <<"USAGE";
Usage: perl $0 [options]

Possible options are:

  --dsn=<db>         The DBI DSN to pass to DBI->connect
  --user=<user>      Use the username <user> for running the test suite;
                     defaults to no username
  --password=<pwd>   Use the password <pwd> for running the test suite;
                     defaults to no password
  --table=<table>    A table to be used as a test table in the DB
                     defaults to dbixl4p
USAGE
  exit 1;
}
