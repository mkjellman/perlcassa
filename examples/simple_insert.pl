#!/usr/bin/perl

##################################################################
# simple example that creates a column family and inserts a value
#
##################################################################

use strict;
use warnings;

use perlcassa;

# create a column family called test_cf into the already existing keyspace named test
my $obj = new perlcassa(
	keyspace	=> 'test',
	hosts		=> ['127.0.0.1']
);

# this takes all valid options such as gc_grace_seconds, max_compaction_threshold, comment etc as well
$obj->column_family(
	action			=> 'create', # valid actions are update, create, or drop
	columnname		=> 'test_cf',
	comparator_type		=> 'CompositeType(UTF8Type,UTF8Type)',
	key_validation_class	=> 'UTF8Type',
	default_validation_class=> 'UTF8Type'
);

my %composite = (
	'values'	=> ['colpt1','colpt2']
);

$obj->insert(
	columnfamily	=> 'test_cf',
	key		=> 'testkey',
	name		=> \%composite,
	value		=> 'my test value',
	ttl		=> 60			# optional, defaults to ttl as defined by cf if not specified
);

$obj->finish();
