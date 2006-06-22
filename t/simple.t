# $Id: simple.t 208 2006-06-21 15:10:33Z martin $
$^W = 1;

push @INC, 't';
require 'lib.pl';
my ($dsn,$user,$password,$table) = get_config();

use Test::More;

if (!defined($dsn) || ($dsn eq "")) {
    plan tests => 6;
} else {
    plan tests => 17;
}

use_ok('DBIx::Log4perl');
use_ok('File::Spec');
use_ok('Log::Log4perl');

my $out;
#########################

my $conf1 = 'example.conf';
my $conf2 = File::Spec->catfile(File::Spec->updir, 'example.conf');

ok ((! -r $conf1) || (! -r $conf2), "Log::Log4perl config exists");
my $conf = $conf1 if (-r $conf1);
$conf = $conf2 if (-r $conf2);

config();

if (!defined($dsn) || ($dsn eq "")) {
    diag("Connection orientated test not run because no database connect information supplied");
    exit 0;
}

my $dbh = DBIx::Log4perl->connect($dsn, $user, $password);
ok($dbh, 'connect to db');
BAIL_OUT("Failed to connect to database - all other tests abandoned")
	if (!$dbh);
ok(check_log(\$out), 'test for log output');

eval {$dbh->do(qq/drop table $table/)};
ok(check_log(\$out), 'drop test table');
ok($dbh->do(qq/create table $table (a int primary key, b char(50))/),
   'create test table');
ok(check_log(\$out), 'test for log output');

my $sth;

ok($sth = $dbh->prepare(qq/insert into $table values (?,?)/),
   'prepare insert');
SKIP: {
	skip "prepare failed", 3 unless $sth;

	ok(check_log(\$out), 'test for log output');

	ok($sth->execute(1, 'one'), 'insert one');
	ok(check_log(\$out), 'test for log output');
};

ok ($dbh->disconnect, 'disconnect');
ok(check_log(\$out), 'test for log output');
