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

plan tests => 14;

require_ok( 'perlcassa' );

my $dbh;

$dbh = new perlcassa(
    'hosts' => [$test_host],
    'keyspace' => $test_keyspace,
);

my $res;

my $param_01 = {
    1 => 'XX',
    2 => 'joe',
    3 => 'bob',
    4 => 1980,
};
eval {
    $res = $dbh->exec("INSERT INTO $test_keyspace.user_profiles (user_id, first_name, last_name, year_of_birth) VALUES (:1, :2, :3, :4);", $param_01);
};
is($res->{type}, 2, "Check type for insert result.");
is($res->{rowcount}, 0, "Check row count for insert result.");
is($res->fetchone(), undef, "Check out of data error with insert result.");

my $param_02 = {
    1 => "100",
    2 => "owens",
    3 => "Mallory",
    4 => 1178683724,
};
eval {
    $res = $dbh->exec("INSERT INTO $test_keyspace.user_profiles (user_id, first_name, last_name, year_of_birth) VALUES (:1, :2, :3, :4);", $param_02);
};
ok($res, "Check type for insert result.");

eval {
    $res = $dbh->exec("SELECT * FROM $test_keyspace.user_profiles LIMIT 2", {'cf'=>'user_profiles'});
}; 
is($res->{rowcount}, 2, "Ensure rowcount works.");
is(keys($res->fetchone() || {}), 4, "Proper number of keys in result hash.");
is(keys($res->fetchone() || {}), 4, "Proper number of keys in result hash.");
is($res->fetchone(), undef, "Out of data.");

my $param_03 = {
    1 => "123",
    2 => "Jane",
    3 => "Doe",
    4 => 1943,
};
$res = $dbh->exec("INSERT INTO $test_keyspace.user_profiles (user_id, first_name, last_name, year_of_birth) VALUES (:1, :2, :3, :4);", $param_03);

$res = $dbh->exec("SELECT * FROM $test_keyspace.user_profiles WHERE user_id = :1", $param_03);
is($res->{rowcount}, 1, "Select single row.");

my $row = $res->fetchone();
is($row->{user_id}->{value}, $param_03->{1}, "Check user_id.");
is($row->{first_name}->{value}, $param_03->{2}, "Check first_name.");
is($row->{last_name}->{value}, $param_03->{3}, "Check last_name.");
is($row->{year_of_birth}->{value}, $param_03->{4}, "Check year_of_birth.");

$dbh->finish();    

