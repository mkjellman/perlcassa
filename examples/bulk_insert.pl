#!/usr/bin/perl

use strict;
use warnings;

use perlcassa;

my $obj = new perlcassa(
	keyspace	=> 'test',
	hosts		=> ['127.0.0.1'],
	columnfamily	=> 'comp_utf8',
	do_not_discover_peers => 1
);

my %bulk = (
	# value => [columnname]
	'EST' => ['MA', 'Boston'],
	'EST' => ['MA', 'Waltham'],
	'PDT' => ['CA', 'Cupertino'],
	'PDT' => ['CA', 'Campbell']
);

$obj->bulk_insert(
	key	=> 'testkey',
	columns => \%bulk,
	ttl	=> 900 # optional
);

$obj->finish();
