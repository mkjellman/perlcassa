#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;
use utf8;

use Data::Dumper;

$|++;

plan tests => 28;

use perlcassa;# qw{prepare_prepared_cql3};

is_deeply([perlcassa::prepare_prepared_cql3("SELECT 'n:hello' FROM 'a \" this place';")],
    ["SELECT 'n:hello' FROM 'a \" this place';", []],
    "String literals.");

my $params_01 = {
    query => "stu'ff",
    kv => "Wear Clause",
};
is_deeply([perlcassa::prepare_prepared_cql3("SELECT :query FROM ks.cf WHERE key = :kv;  ", $params_01)],
    ["SELECT ? FROM ks.cf WHERE key = ?;  "
        , ['query', 'kv']
    ],
    "Parameter binding.");

my $params_02 = {
    query => "yay",
};
is_deeply([perlcassa::prepare_prepared_cql3("select \":query\" from key = :query", $params_02)],
    ["select \":query\" from key = ?", ['query']],
    "Parameter binding and string quoting mixed.");

TODO: {
    local $TODO = "Query Comments are not yet implemented.";
}

my $varint_v = Math::BigInt->new("1000000000000000000001");
my $param_int = {
    biv => 8589934592,
    iv => 7,
    viv => $varint_v,
};
is_deeply([perlcassa::prepare_prepared_cql3("INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test', :biv, :iv, :viv)", $param_int)],
    ["INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test', ?, ?, ?)",
        ['biv', 'iv', 'viv']
    ],
    "Quoting integer types.");

# Check hex to big int conversion
is(perlcassa::Decoder::hex_to_bigint(0, "01"), "1");
is(perlcassa::Decoder::hex_to_bigint(0, "0080"), "128");
is(perlcassa::Decoder::hex_to_bigint(0, "0840"), "2112");
is(perlcassa::Decoder::hex_to_bigint(1, "ff"), "-1");
is(perlcassa::Decoder::hex_to_bigint(1, "fb"), "-5");
is(perlcassa::Decoder::hex_to_bigint(1, "81"), "-127");
is(perlcassa::Decoder::hex_to_bigint(1, "80"), "-128");
is(perlcassa::Decoder::hex_to_bigint(1, "c9ca36523a215fffff"), "-1000000000000000000001");
is(perlcassa::Decoder::hex_to_bigint(1, "f607c81f"), "-167262177");

my $hex01 = "0000002801ade9b959b7cd0009aeccd49a03bf46643c67c8edcd";
my $dec01 = "62831853071.7958647692528676655900576839433879875021";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex01), "org.apache.cassandra.db.marshal.DecimalType"),
    $dec01,
    "Unpack Decimal Type ok, positive large magnitude"
);
my $packed01 = perlcassa::Decoder::pack_val( $dec01, "org.apache.cassandra.db.marshal.DecimalType");
is(unpack("H*", $packed01), $hex01, "Pack Decimal Type ok, positive large magnitude");

my $hex02 = "00000023f607c81f";
my $dec02 = "-0.00000000000000000000000000167262177";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex02), "org.apache.cassandra.db.marshal.DecimalType"),
    $dec02,
    "Unpack Decimal Type ok, negative small magnitude"
);
my $packed02 = perlcassa::Decoder::pack_val( $dec02, "org.apache.cassandra.db.marshal.DecimalType");
is(unpack("H*", $packed02), $hex02, "Pack Decimal Type ok, negative small magnitude");

my $hex03 = "000000237607c81f";
my $dec03 = "0.00000000000000000000000001980221471";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex03), "org.apache.cassandra.db.marshal.DecimalType"),
    $dec03,
    "Unpack Decimal Type ok, positive small magnitude"
);
my $packed03 = perlcassa::Decoder::pack_val( $dec03, "org.apache.cassandra.db.marshal.DecimalType");
is(unpack("H*", $packed03), $hex03, "Pack Decimal Type ok, positive small magnitude");

my $hex04 = "8936523a215fffff";
my $int04 = "-8559563632149528577";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex04), "org.apache.cassandra.db.marshal.IntegerType"),
    $int04,
    "Unpack Integer type, negative even length hex"
);
my $packed04 = perlcassa::Decoder::pack_val( $int04, "org.apache.cassandra.db.marshal.IntegerType");
is(unpack("H*", $packed04), $hex04, "Pack Decimal Type ok, negative even length hex");

my $hex05 = "f8000000000000000000";
my $int05 = "-37778931862957161709568";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex05), "org.apache.cassandra.db.marshal.IntegerType"),
    $int05,
    "Unpack Integer type, negative odd length hex"
);
my $packed05 = perlcassa::Decoder::pack_val( Math::BigInt->new($int05), "org.apache.cassandra.db.marshal.IntegerType");
is(unpack("H*", $packed05), $hex05, "Pack Decimal Type ok, negative odd length hex");

my $hex06 = "00800000000000000000";
my $int06 = "2361183241434822606848";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex06), "org.apache.cassandra.db.marshal.IntegerType"),
    $int06,
    "Unpack Integer type, positive high bit set even length"
);
my $packed06 = perlcassa::Decoder::pack_val( Math::BigInt->new($int06), "org.apache.cassandra.db.marshal.IntegerType");
is(unpack("H*", $packed06), $hex06, "Pack Decimal Type ok, positive high bit set even length");

my $hex07 = "0000000010095dd9267e5007d565f1386b768370000000000000";
my $dec07 = "6000000000000000000000000000000000000000000000000000";
is(perlcassa::Decoder::unpack_val(
        pack("H*", $hex07), "org.apache.cassandra.db.marshal.DecimalType"),
    $dec07,
    "Unpack Decimal Type ok, positive large magnitude"
);
TODO: {
    local $TODO = "Numbers with positive exponents do not get packed correctly.";
    my $packed07 = perlcassa::Decoder::pack_val( $dec07, "org.apache.cassandra.db.marshal.DecimalType");
    is(unpack("H*", $packed07), $hex07, "Pack Decimal Type ok, positive large magnitude");
}

my $hex08 = "0004000361616100066262626262620009e383aae383b3e382b40003646464";
my $list08 = ['aaa','bbbbbb','リンゴ','ddd'];
my $packed08 = perlcassa::Decoder::pack_val( $list08, 'org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)');
is(unpack("H*", $packed08), $hex08, "Pack List of UTF8 ok");

