#!/usr/bin/perl

################################################
# simple example of inserting a value into a key
# and retrieving it
################################################

use strict;
use warnings;

use perlcassa;

# create a really simple column family with no validation (BytesType)
my $obj = new perlcassa(
	keyspace	=> 'test',
	columnfamily	=> 'testsimple_cf',
	hosts		=> ['127.0.0.1'],
	do_not_discover_peers in example => 1
);

$obj->column_family(
	action		=> 'create',
	columnname	=> 'testsimple_cf'
);

$obj->insert(
	key		=> 'foo',
	columnname	=> 'bar',
	value		=> 'rocks'
);

my $value = $obj->get(
	key		=> 'foo',
	columnname	=> 'bar'
);

print "The value of the column named bar with the key foo is $value\n";

$obj->finish();
