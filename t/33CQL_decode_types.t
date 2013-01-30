#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;
use Math::BigInt;
use Math::BigFloat;

use Data::Dumper;

$|++;

use vars qw($test_host $test_keyspace);

# Load default connect values from helper script??
# and actually use them?
$test_host = 'localhost';
$test_keyspace = 'xx_testing_cql';

plan tests => 30;

require_ok( 'perlcassa' );

my $dbh = new perlcassa(
    'hosts' => [$test_host],
    'keyspace' => $test_keyspace,
);
my $res;

# Create table for testing normal types
$res = $dbh->exec("CREATE TABLE $test_keyspace.all_types ( 
    pk text PRIMARY KEY,
    t_ascii ascii,
    t_bigint bigint,
    t_blob blob,
    t_boolean boolean,
    t_decimal decimal, 
    t_double double,
    t_float float,
    t_inet inet,
    t_int int,
    t_text text,
    t_timestamp timestamp,
    t_timeuuid timeuuid,
    t_uuid uuid,
    t_varchar varchar,
    t_varint varint,
) WITH COMPACT STORAGE");
ok($res, "Create test table all_types.");

# Check the text types
$res = $dbh->exec("INSERT INTO all_types (pk, t_ascii, t_text, t_varchar) VALUES ('strings_test', 'v_ascii', 'v_text', 'v_v\xC3\xA1rchar')");
$res = $dbh->exec("SELECT pk, t_ascii, t_text, t_varchar FROM all_types WHERE pk = 'strings_test'");
my $row_text = $res->fetchone();
is($row_text->{t_ascii}->{value}, 'v_ascii', "Check ascii type.");
is($row_text->{t_text}->{value}, 'v_text', "Check text type.");
TODO: {
    # TODO check UTF8 support
    local $TODO = "UTF8 strings are not implemented";
    is($row_text->{t_varchar}->{value}, "v_v\xC3\xA1rchar", "Check varchar type.");
}

# Check boolean true and false
$res = $dbh->exec("INSERT INTO all_types (pk, t_boolean) VALUES ('bool_test', false)");
$res = $dbh->exec("SELECT pk, t_boolean FROM all_types WHERE pk = 'bool_test'");
my $row_01 = $res->fetchone();
is($row_01->{t_boolean}->{value}, 0, "Check boolean false.");
$res = $dbh->exec("INSERT INTO all_types (pk, t_boolean) VALUES ('bool_test', true)");
$res = $dbh->exec("SELECT pk, t_boolean FROM all_types WHERE pk = 'bool_test'");
my $row_02 = $res->fetchone();
is($row_02->{t_boolean}->{value}, 1, "Check boolean true.");

# Check floating point types
my $float1_s = '62831853071.7958647692528676655900576839433879875021';
my $float1 = Math::BigFloat->new($float1_s);
my $param_fp1 = { dv => 1234.5, fv => 9.875, av => $float1, };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test1', :fv, :dv, :av)", $param_fp1);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test1'");
my $row_fp1 = $res->fetchone();
is($row_fp1->{t_double}->{value}, 1234.5, "Check double value.");
is($row_fp1->{t_float}->{value}, 9.875, "Check float value.");
is($row_fp1->{t_decimal}->{value}, $float1_s,
    "Check decimal (arbitrary precision float) value.");

# Check negative floating point types
my $float2_s = '-0.00000000000000000000000000167262177';
my $float2 = Math::BigFloat->new($float2_s);
my $param_fp2 = { dv => -0.000012345, fv => -0.5, av => $float2 };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test2', :fv, :dv, :av)", $param_fp2);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test2'");
my $row_fp2 = $res->fetchone();
is($row_fp2->{t_double}->{value}, -0.000012345, "Check negative double value.");
is($row_fp2->{t_float}->{value}, -0.5, "Check negative float value.");
is($row_fp2->{t_decimal}->{value}, $float2_s,
    "Check negative decimal (arbitrary precision float) value.");


# Check integer types
my $varint_v = Math::BigInt->new("1000000000000000000001");
my $param_int = {
    biv => 8589934592,
    iv => 7,
    viv => $varint_v,
};
$res = $dbh->exec("INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test1', :biv, :iv, :viv)", $param_int);
$res = $dbh->exec("SELECT pk, t_bigint, t_int, t_varint FROM all_types WHERE pk = 'int_test1'");
my $row_int1 = $res->fetchone();
is($row_int1->{t_bigint}->{value}, 8589934592, "Check bigint (64-bit int) value.");
is($row_int1->{t_int}->{value}, 7, "Check int (32-bit int) value.");
is($row_int1->{t_varint}->{value}, "1000000000000000000001",
    "Check varint (arbitrary precision) value.");

# Check negative integer values
my $varint_v2 = Math::BigInt->new("-1000000000000000000001");
my $param_int2 = {
    biv => -8589934592,
    iv => -7,
    viv => $varint_v2,
};
$res = $dbh->exec("INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test2', :biv, :iv, :viv)", $param_int2);
$res = $dbh->exec("SELECT pk, t_bigint, t_int, t_varint FROM all_types WHERE pk = 'int_test2'");
my $row_int2 = $res->fetchone();
is($row_int2->{t_bigint}->{value}, -8589934592, "Check negative bigint (64-bit int) value.");
is($row_int2->{t_int}->{value}, -7, "Check negative int (32-bit int) value.");
is($row_int2->{t_varint}->{value}, "-1000000000000000000001",
    "Check negative varint (arbitrary precision) value.");


# Check inet type, both ipv4 and ipv6
$res = $dbh->exec("INSERT INTO all_types (pk, t_inet) VALUES ( 'inet4_test', '10.9.8.7')");
$res = $dbh->exec("SELECT pk, t_inet FROM all_types WHERE pk = 'inet4_test'");
my $row_inet4 = $res->fetchone();
is($row_inet4->{t_inet}->{value}, "10.9.8.7", "Check inet4 type.");

$res = $dbh->exec("INSERT INTO all_types (pk, t_inet) VALUES ( 'inet6_test', '2001:db8:85a3:42:1000:8a2e:370:7334')");
$res = $dbh->exec("SELECT pk, t_inet FROM all_types WHERE pk = 'inet6_test'");
my $row_inet6 = $res->fetchone();
is($row_inet6->{t_inet}->{value}, "2001:db8:85a3:42:1000:8a2e:370:7334", "Check inet6 type.");


# Create Collections Table for test
$res = $dbh->exec("CREATE TABLE collection_types (pk text PRIMARY KEY, t_list list<int>, t_set set<int>, t_map map<int, int>)");
ok($res, "Create test table collection_types.");

# Test empty collections
$res = $dbh->exec("INSERT INTO collection_types (pk) VALUES ('empty_collection_test')");
$res = $dbh->exec("SELECT pk, t_list, t_set, t_map FROM collection_types WHERE pk = 'empty_collection_test'");
my $row_ec = $res->fetchone();
is_deeply($row_ec->{t_list}->{value}, undef, "Check list collection type (empty).");
is_deeply($row_ec->{t_map}->{value}, undef, "Check map collection type (empty).");
is_deeply($row_ec->{t_set}->{value}, undef, "Check set collection type (empty).");

# Test 3 element list
$res = $dbh->exec("INSERT INTO collection_types (pk, t_list) VALUES ('list_test', [91, 92, 93])");
$res = $dbh->exec("SELECT pk, t_list FROM collection_types WHERE pk = 'list_test'");
my $row_l = $res->fetchone();
is_deeply($row_l->{t_list}->{value}, [91,92,93],
    "Check list collection type.");

# Test set
$res = $dbh->exec("INSERT INTO collection_types (pk, t_set) VALUES ('set_test', {3, 1, 4, 5, 9})");
$res = $dbh->exec("SELECT pk, t_set FROM collection_types WHERE pk = 'set_test'");
my $row_s = $res->fetchone();
is_deeply($row_s->{t_set}->{value}, [1,3,4,5,9],
    "Check set collection type.");

# Test map
$res = $dbh->exec("INSERT INTO collection_types (pk, t_map) VALUES ('map_test', {15: 18, 16: 5, 17: 13, 18: 21, 19: 21})");
$res = $dbh->exec("SELECT pk, t_map FROM collection_types WHERE pk = 'map_test'");
my $row_m = $res->fetchone();
is_deeply($row_m->{t_map}->{value}, {15=>18, 16=>5, 17=>13, 18=>21, 19=>21},
    "Check map collection type.");

# Clean up our tables
$res = $dbh->exec("DROP TABLE all_types");
ok($res, "Drop test table all_types.");
$res = $dbh->exec("DROP TABLE collection_types");
ok($res, "Drop test table collection_types.");



# Still need to implement/fix and test
#  blob
#  timestamp
#  timeuuid
#  uuid
#  counters

# Partial Support. UTF8 does not work correctly
#  text
#  varchar

# Working 
#  ascii
#  bigint
#  boolean
#  decimal
#  double
#  float
#  inet
#  int
#  varint
#  map
#  set
#  list

$dbh->finish();    

