#!/usr/bin/perl

use strict;
use warnings;

use perlcassa;

my $obj = new perlcassa(
	columnfamily	=> 'counterCF',
	keyspace	=> 'test',
	hosts		=> ['127.0.0.1']
);

# you can use add() to both add or subtract from a CounterType

# add 1 to a CounterType
$obj->add(
	key		=> 'testkey1',
	columnname	=> 'testcolumn',
);

# add 3 to a CounterType
$obj->add(
	key		=> 'testkey1',
	columnname	=> 'testcolumn',
	counter		=> 3
);

# subtract 1 to a CounterType
$obj->add(
	key		=> 'testkey1',
	columnname	=> 'testcolumn',
	counter		=> -1
);

$obj->finish();
