#!/usr/bin/perl -w

use strict;
use warnings;
use Test::More;

use Data::Dumper;

$|++;

plan tests => 3;

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

