#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;

use Data::Dumper;

$|++;

plan tests => 13;

use perlcassa;# qw{prepare_inline_cql3};

is(perlcassa::prepare_inline_cql3("SELECT 'n:hello' FROM 'a \" this place';"),
    "SELECT 'n:hello' FROM 'a \" this place';",
    "String literals.");

my $params_01 = {
    query => "stu'ff",
    kv => "Wear Clause",
};
is(perlcassa::prepare_inline_cql3("SELECT :query FROM ks.cf WHERE key = :kv;  ", $params_01),
    "SELECT 'stu''ff' FROM ks.cf WHERE key = 'Wear Clause';  ",
    "Parameter binding.");

my $params_02 = {
    query => "yay",
};
is(perlcassa::prepare_inline_cql3("select \":query\" from key = :query", $params_02),
    "select \":query\" from key = 'yay'",
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
is(perlcassa::prepare_inline_cql3("INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test', :biv, :iv, :viv)", $param_int),
    "INSERT INTO all_types (pk, t_bigint, t_int, t_varint) VALUES ('int_test', '8589934592', '7', '1000000000000000000001')",
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


