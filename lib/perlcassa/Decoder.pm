package perlcassa::Decoder;

use strict;
use warnings;
use base 'Exporter';

our @EXPORT = qw(make_cql3_decoder);

use Cassandra::Cassandra;
use Cassandra::Constants;
use Cassandra::Types;
use Math::BigInt;
use Math::BigFloat;

use Data::Dumper;

# XXX this is yanked from perlcassa.pm. it should only be in one place
# hash that contains pack templates for ValidationTypes
our %simple_unpack = (
	'AsciiType' => 'A*',
	'BooleanType' => 'C',
	'BytesType' => 'a*',
	'DateType' => 'N2',
	'FloatType' => 'f>',
	'DoubleType' => 'd>',
	'Int32Type' => 'l>',
	'LongType' => 'q>',
	'UTF8Type' => 'a*',
	'UUIDType' => 'S'
);
our %complicated_unpack = (
	'IntegerType' => \&unpack_IntegerType,
    'DecimalType' => \&unpack_DecimalType,
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
        $data_type =~ s/org\.apache\.cassandra\.db\.marshal\.//;
    }
    my $value = undef;
    if (defined($column->{value})) {
        $value = unpack_val($packed_value, $data_type),
    }
    my $ret = {
        ttl => $column->{ttl},
        timestamp => $column->{timestamp},
        value => $value,
    };
    return $ret;
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
    } else {
        # It is a complicated type
        my $unpack_sub;
        if (defined($complicated_unpack{$data_type})) {
            $unpack_sub = $complicated_unpack{$data_type}
        } else {
            die("[ERROR] Attempted to unpack unimplemented data type. ($data_type)");
        }
        # TODO IntegerType need to be decoded as Math::BigInt
        $unpacked = $unpack_sub->($packed_value);
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
    return $ret;
}

# Unpack arbitrary precision decimal
# Returns a Math::BigFloat
sub unpack_DecimalType {
    my $packed_value = shift;
    my $data_type = shift;
    my ($exp, $sign, $hex) = unpack("NB1XH*", $packed_value);
    my $mantissa = hex_to_bigint($sign, $hex);
    my $ret = Math::BigFloat->new($mantissa."E-".$exp);
    return $ret;
}

1;
