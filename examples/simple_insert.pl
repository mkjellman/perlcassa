#!/usr/bin/perl

##################################################################
# simple example that creates a column family and inserts a value
#
##################################################################

use strict;
use warnings;

use perlcassa;

# optionally if you want to provide the validation classes manually without having the client
# look them up from the cluster for you, you can provide the following hash (for example if your
# validators on column, key, and value were all BytesType)
#
# my %validators = (
# 	column		=> ['BytesType'],
#	key		=> ['BytesType'],
#	comparator	=> ['BytesType']
# );
# create a column family called test_cf into the already existing keyspace named test
my $obj = new perlcassa(
	keyspace	=> 'test',
	hosts		=> ['127.0.0.1'],
	do_not_discover_peers => 1
	#validators	=> \%validators   # if you are going to manually provide the validation classes from above
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
	columnname	=> \%composite,
	value		=> 'my test value',
	ttl		=> 60			# optional, defaults to ttl as defined by cf if not specified
);

$obj->finish();
