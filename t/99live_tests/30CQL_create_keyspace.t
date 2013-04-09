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

plan tests => 4;

require_ok( 'perlcassa' );

my $dbh;

$dbh = new perlcassa(
    'do_not_discover_peers' => 1,
    'hosts' => [$test_host],
    'keyspace' => 'system',
);

my $res;

TODO: {
    local $TODO = "Need to handle Cassandra::InvalidRequestException.";
    eval {
        $res = $dbh->exec("DROP KEYSPACE $test_keyspace");
    };
    ok($res, "Drop testing keyspace ($test_keyspace).");
}

$res = $dbh->exec("CREATE KEYSPACE $test_keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};");
ok($res, "Create test keyspace ($test_keyspace).");

$res = $dbh->exec(" CREATE TABLE $test_keyspace.user_profiles ( user_id text PRIMARY KEY, first_name text, last_name text, year_of_birth int) WITH COMPACT STORAGE");
ok($res, "Create test table.");

$dbh->finish();    

