#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;

use Data::Dumper;

$|++;

use vars qw($test_host $test_keyspace);

# Load default connect values from helper script??
# and actually use them?
$test_host = 'localhost';
$test_keyspace = 'xx_testing_cql';

plan tests => 2;

require_ok( 'perlcassa' );

my $dbh;

$dbh = new perlcassa(
    'hosts' => [$test_host],
    'keyspace' => $test_keyspace,
);

my $res;

eval {
    $res = $dbh->exec("DROP KEYSPACE $test_keyspace");
};
ok($res, "Drop testing keyspace ($test_keyspace).");

$dbh->finish();    





