#!/usr/bin/perl

use strict;
use warnings;

use perlcassa;

# create our column family with a CompositeType comparator type
my $obj = new perlcassa(
	keyspace	=> 'test',
	hosts		=> ['127.0.0.1']
);


$obj->column_family(
	action		=> 'create',
	columnname	=> 'testcomposite',
	comparator_type => 'CompositeType(AsciiType,BytesType,DateType,FloatType,Int32Type,IntegerType,LongType,UTF8Type)',
	key_validation_class => 'UTF8Type',
	default_validation_class => 'UTF8Type'
);

$obj->finish();


# now let's insert some data
my $obj2 = new perlcassa(
	keyspace	=> 'test',
	columnfamily	=> 'testcomposite',
	hosts		=> ['127.0.0.1']
);

my %composite = (
	'values' => ['test1', 'bytes2', time, 15124444.5, '1234','1234']
);

$obj2->insert(
	key		=> 'testkey',
	columnname	=> \%composite,
	value		=> 'really cool value',
	ttl		=> 60 # optional, defaults to cf defaults if not specified
);

my %composite2 = (
	'values' => ['test2', 'bytes3', time, 15124444.5, '1234','1234']
);

$obj2->insert(
	key		=> 'testkey',
	columnname	=> \%composite2,
	value		=> 'our second really cool value',
	ttl		=> 60 # optional, defaults to cf defaults if not specified
);


# now let's get the data out
my $results = $obj2->get_slice(
	key		=> 'testkey',
	start		=> ['test1', 'bytes2'],
	finish		=> ['test2', 'bytes4'],
	return_expired	=> 0 # optional, when set to 1 returns tombstoned records in the results
);

foreach my $res (@$results) {
	my @name = @{$res->{name}};
	print "[@name] is $res->{value} with ttl $res->{ttl} and time $res->{timestamp}\n";
}

$obj2->finish();
