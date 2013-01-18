package perlcassa::CQL3Result;

use strict;
use warnings;

use perlcassa::Decoder qw{make_cql3_decoder};

use Cassandra::Cassandra;
use Cassandra::Constants;
use Cassandra::Types;

use Data::Dumper;

sub new {
    my ($class, %opt) = @_;
    bless my $self = {
        idx => 0,
        result => undef, 
        type => undef,
        rowcount => undef,
        debug => 0,
    }, $class;
}

##
# Process the Casssandra::CqlResult from a CQL3 call.
#
# This expects the full CqlResult and returns 1 on error, 0 on success. Or it
# Returns:
#   0 on success.
#   1 on error.
#   dies on unrecoverable error. (Please file a bug report)
##
sub process_cql3_results {
    my ($self, $response) = @_;
    my $ret = 1;
    if ($self->{debug}) {
        $self->{response} = $response;
        print STDERR "Response: ".Dumper($response);
    }

    $self->{type} = $response->{type};
    if ($self->{type} == Cassandra::CqlResultType::ROWS) {
        # py cql parses the schema to get a decoder stored
        my $decoder = make_cql3_decoder($response->{schema});

        $self->{result} = [];
        $self->{rowcount} = scalar(@{$response->{rows}});
        # Decode each row into a result
        foreach my $row (@{$response->{rows}}) {
            my %unpacked_row = $decoder->decode_row($row);
            push($self->{result}, \%unpacked_row);
        }
        $ret = 0;
    } elsif ($self->{type} == Cassandra::CqlResultType::VOID) {
        # TODO # may be error may be success. check $@. Cassandra::InvalidRequestException
        $self->{rowcount} = 0;
        $ret = 0;
    } elsif ($self->{type} == Cassandra::CqlResultType::INT) {
        die("[ERROR] Not implemented CqlResultType::INT.");
    } else {
        die("[ERROR] Unknown CQL result type ($self->{type}).");
    }
    return $ret;
}

##
# fetchone is used to grab a row of data from the CQL result
#
# Returns:
#   hash    - If there is a row to return, it returns the row as a hash containing the
#       data from the CQL query.
#   undef   - If there are no more rows to return, undef is returned
##
sub fetchone {
    my $self = shift;
    if ($self->{idx} <= $self->{rowcount}) {
        return @{$self->{result}}[$self->{idx}++];
    }
    return undef;
}

##
# fetchall is used to return the array of rows of data from the CQL result
#
# Returns:
#   array - an array containing a hash of data for each row.
##
sub fetchall {
    my $self = shift;
    return $self->{result};
}

1;
