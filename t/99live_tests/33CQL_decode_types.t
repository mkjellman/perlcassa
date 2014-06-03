#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;
use Math::BigInt;
use Math::BigFloat;
use UUID::Tiny;
use utf8;

use Data::Dumper;

$|++;

use constant DEBUG => 0;

use vars qw($test_host $test_keyspace);

# Load default connect values from helper script??
# and actually use them?
$test_host = 'localhost';
$test_keyspace = 'xx_testing_cql';

plan tests => 49;

require_ok( 'perlcassa' );

my $dbh = new perlcassa(
    'do_not_discover_peers' => 1,
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
$res = $dbh->exec("INSERT INTO all_types (pk, t_ascii, t_text, t_varchar) VALUES ('strings_test', 'v_ascii', 'v_text', 'v_várchar')");
$res = $dbh->exec("SELECT pk, t_ascii, t_text, t_varchar FROM all_types WHERE pk = 'strings_test'");
my $row_text = $res->fetchone();
is($row_text->{t_ascii}, 'v_ascii', "Check ascii type.");
is($row_text->{t_text}, 'v_text', "Check text type.");
ok(Encode::is_utf8($row_text->{t_varchar}), "Type varchar should return as utf8 encoded.");
if (DEBUG) {
    use bytes;
    print "database: '".unpack("H*",$row_text->{t_varchar})."'\n";
    print "expected: '".unpack("H*", "v_várchar")."'\n";
}
is($row_text->{t_varchar}, "v_várchar", "Check varchar type.");

# Check the text types packing
my $str = "0á0ݞ1";
my $param_str02 = { utf8 => $str, ascii => "deadbeef?", };
$res = $dbh->exec("INSERT INTO all_types (pk, t_ascii, t_varchar) VALUES ('strings02_test', :ascii, :utf8)", $param_str02);
$res = $dbh->exec("SELECT * FROM all_types WHERE pk = 'strings02_test'");
my $row_text02 = $res->fetchone();
ok(Encode::is_utf8($row_text02->{t_varchar}), "Type varchar should return as utf8 encoded.");
is($row_text02->{t_varchar}, $str, "Check varchar type.");
if (DEBUG) {
    use bytes;
    print "database: '".join(":", map {ord} split(//,$str))."'\n";
    print "database: '".unpack("H*",$row_text02->{t_varchar})."'\n";
    print "expected: '".unpack("H*",$str)."'\n";
}

#my $json = encode_json({1=>$row_text02->{t_varchar}});
#is($json, '{"1":"0á0ݞ1"}', "Testing json output");
#if (DEBUG) {
#    print "Test: '$json' done\n"; # XXX remove
#    use bytes;
#    print "json out: '".unpack("H*",$json)."'\n";
#    print "expected: '".unpack("H*",'{"1":"0á0ݞ1"}')."'\n";
#}

# Check boolean true and false
$res = $dbh->exec("INSERT INTO all_types (pk, t_boolean) VALUES ('bool_test', false)");
$res = $dbh->exec("SELECT pk, t_boolean FROM all_types WHERE pk = 'bool_test'");
my $row_01 = $res->fetchone();
is($row_01->{t_boolean}, 0, "Check boolean false.");
$res = $dbh->exec("INSERT INTO all_types (pk, t_boolean) VALUES ('bool_test', true)");
$res = $dbh->exec("SELECT pk, t_boolean FROM all_types WHERE pk = 'bool_test'");
my $row_02 = $res->fetchone();
is($row_02->{t_boolean}, 1, "Check boolean true.");

# Check floating point types
my $float1_s = '62831853071.7958647692528676655900576839433879875021';
my $float1 = Math::BigFloat->new($float1_s);
my $param_fp1 = { dv => 1234.5, fv => 9.875, av => $float1, };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test1', :fv, :dv, :av)", $param_fp1);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test1'");
my $row_fp1 = $res->fetchone();
is($row_fp1->{t_double}, 1234.5, "Check double value.");
is($row_fp1->{t_float}, 9.875, "Check float value.");
is($row_fp1->{t_decimal}, $float1_s,
    "Check decimal large (arbitrary precision float) value.");

# Check negative floating point types
my $float2_s = '-0.00000000000000000000000000167262177';
my $float2 = Math::BigFloat->new($float2_s);
my $param_fp2 = { dv => -0.000012345, fv => -0.5, av => $float2 };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test2', :fv, :dv, :av)", $param_fp2);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test2'");
my $row_fp2 = $res->fetchone();
is($row_fp2->{t_double}, -0.000012345, "Check negative double value.");
is($row_fp2->{t_float}, -0.5, "Check negative float value.");
is($row_fp2->{t_decimal}, $float2_s,
    "Check negative decimal small (arbitrary precision float) value.");

# Check floating point types
my $float3_s = '-62831853071.7958647692528676655900576839433879875021';
my $float3 = Math::BigFloat->new($float3_s);
my $param_fp3 = { dv => 1234.5, fv => 9.875, av => $float3, };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test3', :fv, :dv, :av)", $param_fp3);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test3'");
my $row_fp3 = $res->fetchone();
is($row_fp3->{t_double}, 1234.5, "Check double value.");
is($row_fp3->{t_float}, 9.875, "Check float value.");
is($row_fp3->{t_decimal}, $float3_s,
    "Check negative decimal large (arbitrary precision float) value.");

# Check small floating point types
my $float4_s = '0.00000000000000000000000001980221471';
my $float4 = Math::BigFloat->new($float4_s);
my $param_fp4 = { dv => -0.000012345, fv => -0.5, av => $float4 };
$res = $dbh->exec("INSERT INTO all_types (pk, t_float, t_double, t_decimal) VALUES ('float_test4', :fv, :dv, :av)", $param_fp4);
$res = $dbh->exec("SELECT pk, t_float, t_double, t_decimal FROM all_types WHERE pk = 'float_test4'");
my $row_fp4 = $res->fetchone();
is($row_fp4->{t_double}, -0.000012345, "Check negative double value.");
is($row_fp4->{t_float}, -0.5, "Check negative float value.");
is($row_fp4->{t_decimal}, $float4_s,
    "Check decimal small (arbitrary precision float) value.");

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
is($row_int1->{t_bigint}, 8589934592, "Check bigint (64-bit int) value.");
is($row_int1->{t_int}, 7, "Check int (32-bit int) value.");
is($row_int1->{t_varint}, "1000000000000000000001",
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
is($row_int2->{t_bigint}, -8589934592, "Check negative bigint (64-bit int) value.");
is($row_int2->{t_int}, -7, "Check negative int (32-bit int) value.");
is($row_int2->{t_varint}, "-1000000000000000000001",
    "Check negative varint (arbitrary precision) value.");


# Check inet type, both ipv4 and ipv6
$res = $dbh->exec("INSERT INTO all_types (pk, t_inet) VALUES ( 'inet4_test', '10.9.8.7')");
$res = $dbh->exec("SELECT pk, t_inet FROM all_types WHERE pk = 'inet4_test'");
my $row_inet4 = $res->fetchone();
is($row_inet4->{t_inet}, "10.9.8.7", "Check inet4 type.");

$res = $dbh->exec("INSERT INTO all_types (pk, t_inet) VALUES ( 'inet6_test', '2001:db8:85a3:42:1000:8a2e:370:7334')");
$res = $dbh->exec("SELECT pk, t_inet FROM all_types WHERE pk = 'inet6_test'");
my $row_inet6 = $res->fetchone();
is($row_inet6->{t_inet}, "2001:db8:85a3:42:1000:8a2e:370:7334", "Check inet6 type.");


# Create Collections Table for test
$res = $dbh->exec("CREATE TABLE collection_types (pk text PRIMARY KEY, t_list list<int>, t_set set<int>, t_set_text set<text>, t_map map<int, int>)");
ok($res, "Create test table collection_types.");

# Test empty collections
$res = $dbh->exec("INSERT INTO collection_types (pk) VALUES ('empty_collection_test')");
$res = $dbh->exec("SELECT pk, t_list, t_set, t_set_text, t_map FROM collection_types WHERE pk = 'empty_collection_test'");
my $row_ec = $res->fetchone();
is_deeply($row_ec->{t_list}, undef, "Check list collection type (empty).");
is_deeply($row_ec->{t_map}, undef, "Check map collection type (empty).");
is_deeply($row_ec->{t_set}, undef, "Check set collection type (empty).");
is_deeply($row_ec->{t_set_text}, undef, "Check set collection type (empty).");

# Test 3 element list
$res = $dbh->exec("INSERT INTO collection_types (pk, t_list) VALUES ('list_test', [91, 92, 93])");
$res = $dbh->exec("SELECT pk, t_list FROM collection_types WHERE pk = 'list_test'");
my $row_l = $res->fetchone();
is_deeply($row_l->{t_list}, [91,92,93],
    "Check list collection type.");

# Test set
$res = $dbh->exec("INSERT INTO collection_types (pk, t_set) VALUES ('set_test', {3, 1, 4, 5, 9})");
$res = $dbh->exec("SELECT pk, t_set FROM collection_types WHERE pk = 'set_test'");
my $row_s = $res->fetchone();
is_deeply($row_s->{t_set}, [1,3,4,5,9],
    "Check set collection type.");

# Test set of text
my $set_text_params = { set => [ 'aaaaaa','bbbbbb','dddddd','グラニースミス','リンゴ' ] };
$res = $dbh->exec("INSERT INTO collection_types (pk, t_set_text) VALUES ('set_test', :set)", $set_text_params);
$res = $dbh->exec("SELECT pk, t_set_text FROM collection_types WHERE pk = 'set_test'");
my $row_st = $res->fetchone();
is_deeply($row_st->{t_set_text}, ['aaaaaa','bbbbbb','dddddd','グラニースミス','リンゴ'],
    "Check set of text collection type along with packing set.");

# Test map
$res = $dbh->exec("INSERT INTO collection_types (pk, t_map) VALUES ('map_test', {15: 18, 16: 5, 17: 13, 18: 21, 19: 21})");
$res = $dbh->exec("SELECT pk, t_map FROM collection_types WHERE pk = 'map_test'");
my $row_m = $res->fetchone();
is_deeply($row_m->{t_map}, {15=>18, 16=>5, 17=>13, 18=>21, 19=>21},
    "Check map collection type.");


# Test getting value ttl and timestamp
$res = $dbh->exec("INSERT INTO all_types (pk, t_ascii) VALUES ( 'ttl_test', 'to be, or not to be') USING TTL 5");
$res = $dbh->exec("SELECT pk, t_ascii, TTL(t_ascii) FROM all_types WHERE pk = 'ttl_test'");
my $row_ttl = $res->fetchone();
cmp_ok($row_ttl->{"ttl(t_ascii)"}, '<=', 5, "Check retrieving TTL.");

$res = $dbh->exec("INSERT INTO all_types (pk, t_ascii) VALUES ('writetime_test', 'historic occasion') USING TIMESTAMP 1337842800000000");
$res = $dbh->exec("SELECT pk, t_ascii, WRITETIME(t_ascii), TTL(t_ascii) FROM all_types WHERE pk = 'writetime_test'");
my $row_ts = $res->fetchone();
is($row_ts->{"writetime(t_ascii)"}, 1337842800000000, "Check timestamp/writetime support.");

pass("Sleeping to allow TTL to expire.");
sleep(6);
$res = $dbh->exec("SELECT pk, t_ascii, WRITETIME(t_ascii), TTL(t_ascii) FROM all_types WHERE pk = 'ttl_test'");
$row_ttl = $res->fetchone();
is($row_ttl, undef, "Check TTL expiration.");

# Test UUID types
my $param_uuid01 = { tuuid => create_UUID_as_string(UUID_V1), uuid => create_UUID_as_string(UUID_V4) };
$res = $dbh->exec("INSERT INTO all_types (pk, t_timeuuid, t_uuid) VALUES ( 'uuid_test', :tuuid, :uuid)",
    $param_uuid01
);
$res = $dbh->exec("SELECT pk, t_timeuuid, t_uuid FROM all_types WHERE pk = 'uuid_test'");
my $row01_uuid01 = $res->fetchone();
is($row01_uuid01->{t_timeuuid}, $param_uuid01->{tuuid}, "Check Time UUID insert and retrieval.");
is($row01_uuid01->{t_uuid}, $param_uuid01->{uuid}, "Check UUID insert and retrieval.");


# Test timestamp types
my $param_ts01 = { ts => 1374679784862 };
$res = $dbh->exec("INSERT INTO all_types (pk, t_timestamp) VALUES ( 'timestamp_test', :ts)",
    $param_ts01
);
$res = $dbh->exec("SELECT pk, t_timestamp FROM all_types WHERE pk = 'timestamp_test'");
my $row01_ts01 = $res->fetchone();
is($row01_ts01->{t_timestamp}, $param_ts01->{ts}, "Check timestamp insert and retrieval.");


# Test blob types
my $param_blob = { blob => pack("H*", "fedcba98765432100123456789abcdef"), };
$res = $dbh->exec("INSERT INTO all_types (pk, t_blob) VALUES ( 'blob_test', :blob)",
    $param_blob
);
$res = $dbh->exec("SELECT pk, t_blob FROM all_types WHERE pk = 'blob_test'");
my $row01_blob = $res->fetchone();
is($row01_blob->{t_blob}, $param_blob->{blob}, "Check blob insert and retrieval.");


# Clean up our tables
$res = $dbh->exec("DROP TABLE all_types");
ok($res, "Drop test table all_types.");
$res = $dbh->exec("DROP TABLE collection_types");
ok($res, "Drop test table collection_types.");

$dbh->finish();


# Still need to implement/fix and test
#  counters

# Working 
#  ascii
#  bigint
#  blob
#  boolean
#  decimal
#  double
#  float
#  inet
#  int
#  list
#  map
#  set
#  text
#  timestamp
#  timeuuid
#  uuid
#  varchar
#  varint


