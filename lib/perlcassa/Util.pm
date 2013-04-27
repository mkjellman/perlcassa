package perlcassa::Util;

use strict;
use warnings;

use base 'Exporter';
our @EXPORT = qw(isValidIPv4 isValidIPv6);

use Socket;

sub isValidIPv4($) {
	my $ip = shift;

	return $ip =~ /^[\d\.]*$/ && inet_aton($ip);
}

sub isValidIPv6($) {
	my $ip = shift;

	if($ip =~ /^([0-9a-fA-F]{4}|0)(\:([0-9a-fA-F]{4}|0)){7}$/) {
		return 1;
	}
}

