#!/usr/bin/perl

use strict;
use warnings;
use perlcassa::Binary::Socket;
use perlcassa::Binary::Pack;
use perlcassa::Util;
use AnyEvent;

my $obj = new perlcassa::Binary::Socket(
	server => "127.0.0.1",
	#port => 9042 #optional, defaults to 9042
);

#how to use sync
$obj->query_sync("SELECT * FROM test.test", "ONE", \&example_callback);

#how to use async
for(my $i = 0; $i < 2; $i++) {
	$obj->query("SELECT * FROM test.test", "ONE", \&example_callback);
}
$obj->submit();


sub example_callback() {
	my $result = shift;

	use Data::Dumper;
	print Dumper($result);
}
