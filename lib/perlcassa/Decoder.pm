package perlcassa::Decoder;

use strict;
use warnings;
use base 'Exporter';
our @EXPORT = qw(make_cql3_decoder);

use Math::BigInt;
use Math::BigFloat;
use Socket;

use Cassandra::Cassandra;
use Cassandra::Constants;
use Cassandra::Types;

use Data::Dumper;

# XXX this is yanked from perlcassa.pm. it should only be in one place
# hash that contains pack templates for ValidationTypes
our %simple_unpack = (
	'AsciiType' => 'A*',
	'BooleanType' => 'C',
	'BytesType' => 'a*',
	'DateType' => 'q>',
	'FloatType' => 'f>',
	'DoubleType' => 'd>',
	'Int32Type' => 'l>',
	'LongType' => 'q>',
	'UTF8Type' => 'a*',
	'UUIDType' => 'S',
	'CounterColumnType' => 'q>'
);

our %complicated_unpack = (
	'IntegerType' => \&unpack_IntegerType,
	'DecimalType' => \&unpack_DecimalType,
    'InetAddressType' => \&unpack_ipaddress,
);

sub new {
    my ($class, %opt) = @_;
    bless my $self = {
        metadata => undef,
        debug => 0,
    }, $class;
}

##
# Used to create CQL3 column decoder
# 
# Arguments:
#   schema - the Cassandra::CqlMetadata containing the schema
# 
# Returns:
#   A decoder object that can decode/deserialize Cassandra::Columns
##
sub make_cql3_decoder {
    my $schema = shift;
    my $decoder = perlcassa::Decoder->new();
    $decoder->{metadata} = $schema;
    return $decoder;
}

##
# Used to decode a CQL row.
# 
# Arguments:
#   row - a Cassandra::CqlRow
#
# Returns:
#   An hash of hashes, each hash containing the column values
##
sub decode_row {
    my $self = shift;
    my $packed_row = shift;
    my %row;
    for my $column (@{$packed_row->{columns}}) {
        $row{$column->{name}} = $self->decode_column($column);
    }
    return %row;
}

##
# Used to decode a CQL column.
#
# Arguments:
#   column - a Cassandra::Column
# Returns:
#   a hash containing the unpacked column data.
##
sub decode_column {
    my $self = shift;
    my $column = shift;

    my $packed_value = $column->{value};
    my $column_name = $column->{name} || undef;
    my $data_type = $self->{metadata}->{default_value_type};
    if (defined($column_name)) {
        $data_type = $self->{metadata}->{value_types}->{$column_name};
        $data_type =~ s/org\.apache\.cassandra\.db\.marshal\.//g;
    }
    my $value = undef;
    if (defined($column->{value})) {
        $value = unpack_val($packed_value, $data_type),
    }
    if (defined($column->{ttl}) || defined($column->{timestamp})) {
        # The ttl and timestamp Cassandra::Column values are not
        # defined when using CQL3 calls
        die("[BUG] Cassandra returned a ttl or timestamp with a CQL3 column.");
    }
    return $value;
}

##
# Used to unpack values based on a pased in data type. This call will die if
# the data type is unknown.
# 
# Arguments:
#   packed_value - the packed value to unpack
#   data_type - the data type to use to unpack the value
#
# Return:
#   An unpacked value
##
sub unpack_val {
    my $packed_value = shift;
    my $data_type = shift;

    my $unpacked;
    if (defined($simple_unpack{$data_type})) {
        $unpacked = unpack($simple_unpack{$data_type}, $packed_value);
    } elsif ($data_type =~ /^(List|Map|Set)Type/) {
        # Need to unpack a collection of values
        $unpacked = unpack_collection($packed_value, $data_type);
    } elsif (defined($complicated_unpack{$data_type})) {
        # It is a complicated type
        my $unpack_sub = $complicated_unpack{$data_type};
        $unpacked = $unpack_sub->($packed_value);
    } else {
            die("[ERROR] Attempted to unpack unimplemented data type. ($data_type)");
    }
    return $unpacked;
}

# Convert a hex string to a signed bigint
sub hex_to_bigint {
    my $sign = shift;
    my $hex = shift;
    my $ret;
    if ($sign) {
        # Flip the bits... Then flip again... 
        # I think Math::BigInt->bnot() is broken
        $hex =~ tr/0123456789abcdef/fedcba9876543210/;
        $ret = Math::BigInt->new("0x".$hex)->bnot();
    } else {
        $ret = Math::BigInt->new("0x".$hex);
    }
    return $ret;
}

# Unpack arbitrary precision int
# Returns a Math::BigInt
sub unpack_IntegerType {
    my $packed_value = shift;
    my $data_type = shift;
    my $ret = hex_to_bigint(unpack("B1XH*", $packed_value));
    my $unpacked_int = $ret->bstr();
    return $unpacked_int;
}

# Unpack arbitrary precision decimal
# Returns a Math::BigFloat
sub unpack_DecimalType {
    my $packed_value = shift;
    my $data_type = shift;
    my ($exp, $sign, $hex) = unpack("NB1XH*", $packed_value);
    my $mantissa = hex_to_bigint($sign, $hex);
    my $ret = Math::BigFloat->new($mantissa."E-".$exp);
    my $unpacked_dec = $ret->bstr();
    return $unpacked_dec;
}

# Unpack inet type
# Returns a string
sub unpack_ipaddress {
    my $packed_value = shift;
    my $data_type = shift;
    my $len = length($packed_value);
    my $ret;
    if ($len == 16) {
        # Unpack ipv6 address
        $ret = Socket::inet_ntop(Socket::AF_INET6, $packed_value);
    } elsif ($len == 4) {
        $ret = Socket::inet_ntop(Socket::AF_INET, $packed_value);
    } else {
        die("[ERROR] Invalid inet type.");
    }
    return $ret;
}

# Unpack a collection type. List, Map, or Set
# Returns:
#   array - for list
#   array - for set
#   hash - for map
#
sub unpack_collection {
    my $packed_value = shift;
    my $data_type = shift;
    my $unpacked;
    my ($base_type, $inner_type) = ($data_type =~ /^([^)]*)\(([^)]*)\)/);

    # "nX2n/( ... )"
    # Note: the preceeding is the basic template for collections.
    # The template code grabs a 16-bit unsigned value, which is the number of
    # items/pairs in the collection. Then it goes backward 2 bytes (16 bits)
    # and grabs 16-bit value again, but uses it to know how many items/pairs
    # to decode
    if ($base_type =~ /^(List|Set)Type/) {
        # Handle the list and set. They are bascally the same
        # Each item is unpacked as raw bytes, then unpacked by our normal
        # routine
        my ($count, @values) = unpack("nX2n/(n/a)", $packed_value);
        $unpacked = [];
        for (my $i = 0; $i < $count; $i++) {
            my $v = unpack_val($values[$i], $inner_type);
            push(@{$unpacked}, $v);
        }

    } elsif ($base_type =~ /^MapType/) {
        # Handle the map types
        # Each pair is unpacked as two groups of raw bytes, then unpacked by
        # our normal routines
        my ($count, @values) = unpack("nX2n/(n/a n/a)", $packed_value);
        my @inner_types = split(",", $inner_type);
        $unpacked = {};
        for (my $i = 0; $i < $count; $i++) {
            my $k = unpack_val($values[($i*2+0)], $inner_types[0]);
            my $v = unpack_val($values[($i*2+1)], $inner_types[1]);
            $unpacked->{$k} = $v;
        }

    } else {
        die("[BUG] You broke it. File a bug... What is '$data_type'?");
    }
    return $unpacked;
}

1;
