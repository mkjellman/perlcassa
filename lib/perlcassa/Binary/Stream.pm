package perlcassa::Binary::Stream;

use strict;
use warnings;

sub new() {
	my ($class, %opt) = @_;

	bless my $self = {
		inuse => 0,
		timestamp => undef,
		id => ${opt}{id} || 0,
	}, $class;

	return $self;
}

1
