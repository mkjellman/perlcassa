package perlcassa::Decoder;

use strict;
use warnings;
use base 'Exporter';

our @EXPORT = qw(make_cql3_decoder);

use Cassandra::Cassandra;
use Cassandra::Constants;
use Cassandra::Types;

use Data::Dumper;

# XXX this is yanked from perlcassa.pm. it should only be in one place
# hash that contains pack templates for ValidationTypes
our %validation_map = (
	'AsciiType'	=> 'A*',
	'BooleanType'	=> 'C',
	'BytesType' 	=> 'a*',
	'DateType' 	=> 'N2',
	'FloatType' 	=> 'f',
	'Int32Type' 	=> 'N',
	'IntegerType' 	=> 'N2',
	'LongType' 	=> 'N2',
	'UTF8Type' 	=> 'a*',
	'UUIDType'	=> 'S'
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
    if (!defined($column->{value})) {
        die("[ERROR] The value to decode must be defined.");
    }

    my $packed_value = $column->{value};
    my $column_name = $column->{name} || undef;
    my $data_type = $self->{metadata}->{default_value_type};
    if (defined($column_name)) {
        $data_type = $self->{metadata}->{value_types}->{$column_name};
        $data_type =~ s/org\.apache\.cassandra\.db\.marshal\.//;
    }
    my $ret = {
        ttl => $column->{ttl},
        timestamp => $column->{timestamp},
        value => unpack_val($packed_value, $data_type),
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

    my $unpack_str = $validation_map{$data_type} 
        or die("[ERROR] Attempted to unpack unimplemented data type. ($data_type)");
    my $unpacked = unpack($unpack_str, $packed_value);
    return $unpacked;
}
