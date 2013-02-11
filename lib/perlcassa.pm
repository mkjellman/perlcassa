package perlcassa;

=head1 NAME

perlcassa - Perl Client for Apache Cassandra

=head1 VERSION

v0.05

=head1 SYNOPSIS

    use perlcassa;
    
    my $obj = new perlcassa(
        'columnfamily' => 'myCF',
        'keyspace' => 'myKeyspace',
        'seed_nodes' => ['host1.cassandra.local',
                     'host2.cassandra.local',
                     'host3.cassandra.local'],
        #optional
        'write_consistency_level' => Cassandra::ConsistencyLevel::QUORUM,
        'read_consistency_level' => Cassandra::ConsistencyLevel::QUORUM,
        'port' => '9160'
    );
    
    my %composite = ('values' => ['name_pt1', 'name_pt2']);
    
    $obj->insert(
        'key' => 'myKey',
        'columnname' => \%composite,
        'value' => 'myVal'
    );
    
    $obj->get(
        'key' => 'myKey',
        'columnname' => 'myColumn'
    );
    
    $obj->get_slice(
        'key' => 'myKey',
        'start' => ['name_pt1'],
        'finish' => ['name_pt2','name_pt2_c'],
        'start_equality' => 'equal', #optional (defaults to equal, options: equal, less_than_equal, or greater_than_equal)
        'finish_equality' => 'greater_than_equal' #optional (defaults to greater_than_equal, options: equal, less_than_equal, or greater_than_equal)
    );
    
    $obj->get_range_slices(
        key_start => '',
        key_finish => '',
        column_start => ['colpt1','a'],
        column_finish => ['thiscol'],
        key_max_count => 10000,
        buffer_size => 100
    );
    
    my %bulk = (
        #value => [columnname]
        'test' => ['name_pt1', 'name_pt2'],
        'test2' => ['name_pt3', 'name_pr4']
    );
    
    $obj->bulk_insert(
        'key' => 'testkey'
        'columns' => \%bulk
    );

    my $result = $obj->exec(
        "SELECT key, col01 FROM myKeyspace.myCF_CQL3 WHERE key = :key_value",
        {key_value => 'mykey'}
    );
    my $row = $result->fetchone();
    print "Row key, col01: ".$row->{key}.", ".$row->{col01}."\n";


=head1 REQUIRES

Perl5.10, Thrift::XS, Time::HiRes 

=head1 EXPORTS

Nothing

=head1 DESCRIPTION

perlcassa is a native Perl client for interfacing with Apache Cassandra. It is essentially an API for Apache Thrift. It intelligently deals with CompositeType columns and ValidationClasses and encodes and packs them appropriately for the columnfamily specified. perlcassa deals with connection pooling, automatic retrying of insertions, automatic serialization and deserialization of primitive data types to pass column validation classes and more.

Although other Perl Cassandra clients exist such as Cassandra::Lite and Net::Cassandra they have not been updated for many of the changes in Cassandra releases >0.80. They also do not serialize and deserialize data making them not much more than an abstraction of the base Thrift calls. In my experence the difficulty lies in validation classes and being fault tolerant, not abstracting the Thrift code.

The module name perlcassa follows the naming convention of other Cassandra clients such as phpcassa and pycassa. This module is included on CPAN for convinence however, please see https://github.com/mkjellman/perlcassa for active development.

Note: This package does not support SuperColumns. Please look into CompositeType Comparators instead.

=head1 METHODS

=head2 Creation

=over 4

=item new perlcassa()

Creates a new Apache Cassandra Perl Client

=back

=head1 TODO

=over 4

=item *
better documentation

=item *
better handling thrift exceptions to try from another provided Cassandra instance/host automagically

=item *
general performance optimizations

=item *
auto retry failures where the node is up when the client is created but there is an exception such as a timeout on insert

=item *
better CQL3 support

=back

=head1 ACKNOWLEDGEMENTS

This wouldn't have been possible without help from friend and colleague BJ Black.

=head1 AUTHOR

Michael Kjellman, mkjellman@barracuda.com
Colin Blower, cblower@barracuda.com

=head1 COPYRIGHT & LICENSE

Copyright 2013 Michael Kjellman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

use strict;
use warnings;

our $VERSION = '0.041';

use perlcassa::Client qw/setup close_conn client_setup/;
use perlcassa::CQL3Result;

use Cassandra::Cassandra;
use Cassandra::Constants;
use Cassandra::Types;

use utf8;
use Encode;
use Time::HiRes qw ( time );
use Math::BigInt;

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

# Get the time in microsecond precision
sub ustime {
    return int(1000*1000*time());
}

sub new() {
	my ($class, %opt) = @_;

	if (!defined($opt{hosts}) && defined($opt{do_not_discover_peers})) {
		die('you must provide at least one cassandra host');
	}

	if (defined($opt{hosts}) && !defined($opt{do_not_discover_peers})) {
		die('hosts has been depricated for seed_nodes. please create object with \'do_not_discover_peers\' if you want to still manually provide hosts');
	}

	if (!defined($opt{hosts}) && !defined($opt{seed_nodes})) {
		die('you must provide at least one seed_node or create the object with \'do_not_discover_peers\' and pass \'hosts\' in the object creation');
	}

	bless my $self = {
		client => undef,
		transport => undef,
		protocol => undef,
		socket => undef,
		hosts => $opt{hosts}, # user provided list of all cassandra nodes potentially avaliable for reads or writes
		seed_nodes => $opt{seed_nodes}, # user lists of cassandra nodes to contact to get all other peers in cluster 
		port => $opt{port} || '9160',
		keyspace => $opt{keyspace} || undef,
		columnfamily => $opt{columnfamily} || undef,
		comparators => undef,
		read_consistency_level => $opt{read_consistency_level} || Cassandra::ConsistencyLevel::ONE,
		write_consistency_level => $opt{write_consistency_level} || Cassandra::ConsistencyLevel::ONE,
		debug => $opt{debug} || 0,
		timeout => $opt{timeout} || undef,
		validators => $opt{validators} || undef,
		request_count => 0,
		availablehosts => {},
		failure_thread_running => 0,
		max_retry => $opt{max_retry} || 2, # max number of times to try and connect for this request
		donotpack => $opt{donotpack} || 0, #will insert with raw values and not autopack values
		do_not_discover_peers => $opt{do_not_discover_peers} || undef
	}, $class;

	# generate the inital randomized server list
	$self->perlcassa::Client::generate_host_hashes();
	$self->perlcassa::Client::generate_server_list();

	return $self;
}


#####################################################################################################
# column_family() allows you to create, update, or drop a column family
#
# $obj->column_family(
# 	'action'	  	 => 'create', # or 'update' or 'drop'
#	'columnname'	 	 => 'cfname',
#	'keyspace'	  	 => 'myKeyspace', #optional and not needed it specified in object creation
#	'comparator_type' 	 => 'CompositeType(UTF8Type, UTF8Type)',
#	'key_validation_class 	  => 'UTF8Type',
#	'default_validation_class => 'UTF8Type'
# );
#####################################################################################################
sub column_family() {
	my ($self, %opts) = @_; 

	my $action 			= $opts{action} || '';

	if (!defined($action) || $action eq '' || $action !~ /(update|create|drop)/i) {
		die("invalid action [$action] specified. must be either update, create, or drop");
	}

	my $keyspace 			= $opts{keyspace} || $self->{keyspace};
	my $name			= $opts{columnname} || undef;
	my $min_compaction_threshold	= $opts{min_compaction_threshold} || undef;
	my $gc_grace_seconds		= $opts{gc_grace_seconds} || undef;
	my $default_validation_class	= $opts{default_validation_class} || undef;
	my $max_compaction_threshold	= $opts{max_compaction_threshold} || undef;
	my $read_repair_chance		= $opts{read_repair_chance} || 1;
	my $key_validation_class	= $opts{key_validation_class} || undef;
	my $compaction_strategy_options	= $opts{compaction_strategy_options} || undef;
	my $comparator_type		= $opts{comparator_type} || 'BytesType';
	my $compaction_strategy		= $opts{compaction_strategy} || undef;
	my $column_type			= $opts{column_type} || 'Standard';
	my $replicate_on_write		= $opts{replicate_on_write} || undef;
	my $compression_options		= $opts{compression_options} || undef;
	my $subcomparator_type		= $opts{subcomparator_type} || undef;
	my $column_metadata		= $opts{column_metadata} || undef;
	my $key_alias			= $opts{key_alias} || undef;
	my $comment			= $opts{comment} || undef;		

	if ($action eq 'create') {
		$self->setup($self->{keyspace});
	} else {
		$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $name);
		$self->{client}->set_keyspace($keyspace);
	}

	my $cf_def = new Cassandra::CfDef();
	$cf_def->keyspace($keyspace);
	$cf_def->name($name);
	$cf_def->comparator_type($comparator_type);
	$cf_def->key_validation_class($key_validation_class);
	$cf_def->default_validation_class($default_validation_class);

	$cf_def->min_compaction_threshold($min_compaction_threshold);
	$cf_def->gc_grace_seconds($gc_grace_seconds);
	$cf_def->max_compaction_threshold($max_compaction_threshold);
	$cf_def->read_repair_chance($read_repair_chance);
	$cf_def->compaction_strategy_options($compaction_strategy_options);
	$cf_def->compaction_strategy($compaction_strategy);
	$cf_def->column_type($column_type);
	$cf_def->replicate_on_write($replicate_on_write);
	$cf_def->compression_options($compression_options);
	$cf_def->subcomparator_type($subcomparator_type);
	$cf_def->column_metadata($column_metadata);
	$cf_def->key_alias($key_alias);
	$cf_def->comment($comment);


	if ($action =~ /create/i) {
		$self->{client}->system_add_column_family($cf_def);
		$self->finish();
	} elsif ($action =~ /update/i) {
		my %cf_describe = $self->describe_columnfamily('columnfamily' => $name);
		$cf_def->id($cf_describe{id});
		$self->{client}->system_update_column_family($cf_def);
	} elsif ($action =~ /drop/i) {
		$self->{client}->system_drop_column_family($name);
	} else {
		die('[ERROR] This should never happen. Please file a bug report.');
	}
}

##
# the CQL3 exec call
# 
# $obj->exec("SELECT * FROM users WHERE state='UT' AND birth_year = 1970");
#
# $obj->exec("SELECT * FROM users WHERE state=:state AND birth_year = :by", {state => 'UT', by => 1970} );
#
# Arguments:
#   query - a string specifying the query to be run
#   params - a hash of parameters to be substituted into the query
#   attr - a hash of attributes used to specify options. No options work currently
#
# Returns:
#   a perlcassa:CQL3Result object
##
sub exec() {
	my ($self, $query, $params) = @_;

	# Bind parameters
	my $prepared_query = prepare_inline_cql3($query, $params);

	my $keyspace = $self->{keyspace};
	$self->client_setup('keyspace' => $keyspace);

	my $client = $self->{client};

	my $query_result;
	if (defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "CQL Query Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$query_result = $client->execute_cql3_query(
			$prepared_query,
			Cassandra::Compression::NONE,
			Cassandra::ConsistencyLevel::ONE
		);
		alarm($alarm);
	} else {
		$query_result = $client->execute_cql3_query(
			$prepared_query,
			Cassandra::Compression::NONE,
			Cassandra::ConsistencyLevel::ONE
		);
	}

	my $resp = perlcassa::CQL3Result->new();
	$resp->process_cql3_results($query_result);

	return $resp;
}

##
# Replace each :param_name with the quoted value of $params{param_name}
# Note comments do not work, but string literals should.
##
sub prepare_inline_cql3 {
    my $query = shift;
    my $params = shift || {};
    my $attr = shift || {};
    my %h_params = %$params;

    # Split on string literals. No nesting
    my @split_query = split(/('[^']*'|"[^"]*")/, " ".$query." ");

    # TODO split on comments as well
    my @prepared;
    for my $part (@split_query) {
        if (defined($part)) {
            # Check if it is string literal
            if ($part =~ /^('|")/) {
                push(@prepared, $part);
            } else {
                # Replace each parameter with quoted and escaped version
                # Note this could probably be made faster if needed
                # maybe rewritten for readability too...
                while ($part =~ s/(\W):(\w+)(?=\W)/$1."'"._escape_quotes($h_params{$2})."'"/e) {}
                push(@prepared, $part);
            }
        }
    }
    my $pq = join('', @prepared);
    $pq =~ s/^\ |\ $//g; # remove the whitespace added at beginning
    return $pq;
}

# CQL3 requires quotes to be escaped by doubling them.
sub _escape_quotes() {
    my $str = shift;
    $str =~ s/'/''/g;
    return $str;
}


#####################################################################################################
# execute() allows you to run CQL queries against Apache Cassandra
#
# $obj->execute('SELECT * FROM users WHERE state=\'UT\' AND birth_date > 1970);
#####################################################################################################
sub execute() {
	my ($self, $query) = @_;

	my $keyspace = $self->{keyspace};
	
	$self->client_setup('keyspace' => $keyspace);

	my $client = $self->{client};

	my $return;
	if (defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "CQL Query Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$return = $client->execute_cql_query($query, Cassandra::Compression::NONE);
		alarm($alarm);
	} else {
		$return = $client->execute_cql_query($query, Cassandra::Compression::NONE);
	}
	
	return $return;
}

#####################################################################################################
# describe_all_columnfamilies() returns a hash of hashes describing all columnfamilies in a keyspace
#####################################################################################################
sub describe_all_columnfamilies() {
	my ($self, %opts)  = @_;

	my $keyspace = $opts{keyspace} || $self->{keyspace};
	
	$self->client_setup('keyspace' => $keyspace);
	my $client = $self->{client};

	my $keyspacedesc = $client->describe_keyspace($keyspace);
	my $columnfamilydesc = $keyspacedesc->{cf_defs};

	my %return;
	foreach my $cf (@{$columnfamilydesc}) {
		my %cf = %$cf;

		my $name = $cf{name};
		foreach my $key (keys(%cf)) {
			$return{$name}{$key} = $cf{$key};	
		}
	}

	return %return;
}

#####################################################################################################
# describe_columnfamily() returns a hash with elements of the columnfamily passed
#####################################################################################################
sub describe_columnfamily() {
	my ($self, %opts)  = @_;

	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	
	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);

	my $client = $self->{client};
	my $keyspacedesc = $client->describe_keyspace($keyspace);
	my $columnfamilydesc = $keyspacedesc->{cf_defs};

	my %return;
	foreach my $cf (@{$columnfamilydesc}) {
		my %cf = %$cf;
		my $name = $cf{name};

		if ($name eq $column_family) {
			foreach my $key (keys(%cf)) {
				$return{$key} = $cf{$key};
			}
		}
	}

	# we didn't find a cf in the keyspace provided that the object or call was provided with
	if (!%return) {
		die("Unable to find the column family $column_family in $keyspace");
	}

	return %return;
}

#####################################################################################################
# get_validators() returns a hash of validators for a particular column family
# 	(Key Validation Class, Default column value validator, Comparator Type)
#
# $obj->get_comparatortype();
#####################################################################################################
sub get_validators() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace =  $opts{keyspace} || $self->{keyspace};

	my %columndesc = $self->describe_columnfamily(
		'columnfamily' => $column_family,
		'keyspace' => $keyspace
	);

	# get comparator_type
	my $comparatortype = $columndesc{comparator_type};
	my @types;
	if ($comparatortype =~ /^org\.apache\.cassandra\.db\.marshal\.CompositeType\((.*?)\)$/) {
		my $type = $1;
		$type =~ s/org\.apache\.cassandra\.db\.marshal\.//g;
		@types = split(/,/, $type);
	} else {
		$comparatortype =~ s/org\.apache\.cassandra\.db\.marshal\.//g;
		push (@types, $comparatortype);
	}

	# get Key Validation Class
	my $key_validationclass = $columndesc{key_validation_class};
	$key_validationclass =~ s/org\.apache\.cassandra\.db\.marshal\.//g;

	my @key_validators;
	push (@key_validators, $key_validationclass);

	# deal with validation if we have column metadata defined on column family
	my $column_metadata = $columndesc{column_metadata};
	my %metadata_validators;

	if (defined($column_metadata)) {
		foreach my $cmetadata (@{$column_metadata}) {
			my $class = $cmetadata->{validation_class};
			$class =~ s/org\.apache\.cassandra\.db\.marshal\.//g;
			$metadata_validators{$cmetadata->{name}} = $class;
		}
	}

	# get Default column value validator
	my $default_columnvalidation = $columndesc{default_validation_class};
	$default_columnvalidation =~ s/org\.apache\.cassandra\.db\.marshal\.//g;

	my @column_validators;
	push (@column_validators, $key_validationclass);

	my %validators = (
		'column'	=> \@column_validators,
		'key'		=> \@key_validators,
		'comparator'	=> \@types,
		'metadata'	=> \%metadata_validators	
	);

	return \%validators;
		
}

##########################################################################
# insert() adds or updates a key or a column to a key
#
##########################################################################
sub insert() {
	my ($self, %opts) = @_;
	#TODO: validate this is not an add

	if (!defined($opts{key})) {
		die('[ERROR] Key must be defined');
	}

	if (!defined($opts{columnname})) {
		die('[ERROR] Columnname must be defined');
	}

	if (!defined($opts{value})) {
		warn('[WARN] Value was not defined');
	}
	
	eval {	
		if (defined($self->{timeout})) {
			local $SIG{ALRM} = sub { die "Insert timed out"; };
			my $alarm = alarm($self->{timeout});
			$self->_call("insert", %opts);
			alarm($alarm);
		} else {
			$self->_call("insert", %opts);
		}

		close_conn();
	};

	if ($@) {
		my @errors = $@;
		
		if (defined($errors[0])) {
			die("[ERROR] Insert was unsuccessful: @errors");
		} else {
			my %error = %{$@};
			die("[ERROR] Insert was unsucessful: $error{why}");
		}
	}
}

##########################################################################
# remove() lets you remove a key
#
##########################################################################
sub remove() {
	my ($self, %opts) = @_;

	eval {
		if (defined($self->{timeout})) {
			local $SIG{ALRM} = sub { die "Remove timed out"; };
			my $alarm = alarm($self->{timeout});
			$self->_call("remove", %opts);
			alarm($alarm);
		} else {	
			$self->_call("remove", %opts);
		}

		close_conn();
	};

	if ($@) {
		my %error = %{$@};
		die("[ERROR] Remove was unsuccessful: $error{why}");
	}

}

##########################################################################
# add() is to adding and subtracting from CounterType columns
#
##########################################################################
sub add() {
	my ($self, %opts) = @_;

	# validate this is a commutative operation
	if (defined($opts{counter}) && $opts{counter} !~ /[\d]+/) {
		die("[ERROR] counter must be numeric in value");
	}

	# we should never have value specified as a param
	if (defined($opts{value})) {
		warn('[WARNING] add() is a commutative operation. you should not provide a value. instead you may provide a \'counter\' value to decremenet by');
	}

	# if counter param with amount not specified default to 1
	if (!defined($opts{counter})) {
		$opts{counter} = 1;
	}

	eval {
		if (defined($self->{timeout})) {
			local $SIG{ALRM} = sub { die "Add Timed Out"; };
			my $alarm = alarm($self->{timeout});
			$self->_call("add", %opts);
		} else {
			$self->_call("add", %opts);
		}

		close_conn();
	};

	if ($@) {
		my %error = %{$@};
		die("[ERROR] Add Failed: $error{why}");
	}
}

###########################################################################
# _call() does the actual calls to thrift. should not be called directly
# instead use insert(), remove(), or add() etc
#
###########################################################################
sub _call() {
	my ($self, $thrift_operation, %opts) = @_;

	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $columnfamily = $opts{columnfamily} || $self->{columnfamily};
	my $consistencylevel = $opts{consistency_level} || $self->{write_consistency_level};
	my $counter = $opts{counter} || 1; # default to increment counter by 1 if counter param not passed
	my $key = $opts{key};
	my $name = $opts{columnname};
	my $value = $opts{value};
	my $ttl = $opts{ttl};

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $columnfamily);
	my $client = $self->{client};

	# pack the key to comply with the key validation class on this column family
	my %keyhash = ('values' => [$key]);
	my $packedkey = $self->_pack_values(\%keyhash, $columnfamily, 'key');

	my $column_parent;
	my $packedvalue;
	
	# a remove operation only takes a key, so no need wasting time packing other stuff passed needlessly
	unless ($thrift_operation eq 'remove') {
		# pack the column

		# deal with the condition that we may just be passed a scalar for a simple column name
		unless (ref($name) eq "HASH") {
			my %columnnamehash = ('values' => [$name]);
			$name = $self->_pack_values(\%columnnamehash, $columnfamily, 'column');
		} else {
			$name = $self->_pack_values($name, $columnfamily, 'column');
		}


		# if the operation is to a CounterColumn we wont have a value so don't pack it
		unless ($thrift_operation eq 'add') {
			# pack the value to comply with the default column family validator on this column family
			my %valuehash = ('values' => [$value]);
			$packedvalue = $self->_pack_values(\%valuehash, $columnfamily, 'value', $name);
		}
	}

		$column_parent = new Cassandra::ColumnParent({column_family => $columnfamily});

	if ($thrift_operation eq 'add') {
		# if this is a counter column, we need to do an add() not an insert()
		my $countercolumn = new Cassandra::CounterColumn();

		$countercolumn->{name} = $name;
		$countercolumn->{value} = $counter;

		$client->add($packedkey, $column_parent, $countercolumn, $consistencylevel);
	} elsif ($thrift_operation eq 'remove') { 

		$client->remove($packedkey, $column_parent, ustime, $consistencylevel);
	} else {
		my @mutations;

		# first create the column	
		my $column = new Cassandra::Column();
		$column->{name} = $name;
		$column->{value} = $packedvalue;
		$column->{timestamp} = ustime;

		if(defined($ttl)){
			$column->{ttl} = $ttl; 
		}

		# create a ColumnOrSuperColumn object to put the Column in
		my $c_or_sc = new Cassandra::ColumnOrSuperColumn();
		$c_or_sc->{column} = $column;

		# pass the whole thing into a mutation
		my $mutation = new Cassandra::Mutation();
		$mutation->{column_or_supercolumn} = $c_or_sc;

		# store up all the mutations for this key with all the columns into an array
		push (@mutations, $mutation);

		$client->batch_mutate( { $packedkey => { $columnfamily => \@mutations }}, $consistencylevel);
	}

}


sub bulk_insert() {
	my ($self, %opts) = @_;

	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $columnfamily = $opts{columnfamily} || $self->{columnfamily};
	my $consistencylevel = $opts{consistency_level} || $self->{write_consistency_level};
	my $key = $opts{key};
	my $columns = $opts{columns};
	my $ttl = $opts{ttl};

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $columnfamily);
	my $client = $self->{client};

	my %columns = %$columns;
	my %packedbulk;
	foreach my $value (sort(keys(%columns))) {
		if ($self->{donotpack} == 1) {
			$packedbulk{$value} = @{$columns{$value}}[0];
		} else {
			my %valuehash = ('values' => \@{$columns{$value}});
			my $packedvalue = $self->_pack_values(\%valuehash, $columnfamily, 'value', $value);

			my %columnhash = ('values' => [$value]);
			my $packedname = $self->_pack_values(\%columnhash, $columnfamily, 'column');
		
			$packedbulk{$packedname} = $packedvalue;
		}
	}

	my @mutations;

	foreach my $key (sort(keys(%packedbulk))) {
		# first create the column	
		my $column = new Cassandra::Column();
		$column->{name} = $key;
		$column->{value} = $packedbulk{$key};
		$column->{timestamp} = ustime;

		if (defined($ttl)) {
			$column->{ttl} = $ttl;
		}

		# create a ColumnOrSuperColumn object to put the Column in
		my $c_or_sc = new Cassandra::ColumnOrSuperColumn();
		$c_or_sc->{column} = $column;

		# pass the whole thing into a mutation
		my $mutation = new Cassandra::Mutation();
		$mutation->{column_or_supercolumn} = $c_or_sc;

		# store up all the mutations for this key with all the columns into an array
		push (@mutations, $mutation);
	}

	if (defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Batch Mutate Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$client->batch_mutate( { $key => { $columnfamily => \@mutations }}, $consistencylevel);
		alarm($alarm);
	} else {
		$client->batch_mutate( { $key => { $columnfamily => \@mutations }}, $consistencylevel);
	}
}


#####################################################################################################
# _get_column() returns the literal column family from thrift. This data still needs to be deserailized
#####################################################################################################
sub _get_column() {
	my ($self, $column_family, $column, $super_column, $key, $consistencylevel) = @_;

	my $client = $self->{client};

	my $column_path = new Cassandra::ColumnPath();
	$column_path->{column_family} = $column_family;
	$column_path->{column} = $column if (defined $column);
	$column_path->{super_column} = $super_column if (defined $super_column);

	my $res;
	if (defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Get Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$res = $client->get($key, $column_path, $consistencylevel);
		alarm($alarm);
	} else {
		$res = $client->get($key, $column_path, $consistencylevel);
	}

	return $res;
}

#####################################################################################################
# get() allows you to pull out a key/column pair from cassandra. The client will deserialize the data
# and will return a hash containing the key, column, name, and value
#
# $obj->get(
#	'columnfamily'  => 'myCF', #optional if provided in object creation
#	'keyspace'	=> 'myKeyspace', #optional if provided in object creation
#	'key'		=> 'myKey',
#	'columnname'	=> 'myColumn' 
# );
#####################################################################################################
sub get() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $key = $opts{key};
	my $column = $opts{columnname};
	my $consistencylevel = $opts{consistency_level} || $self->{read_consistency_level};

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);

	my %keyhash = ('values' => [$key]);
	my $packedkey = $self->_pack_values(\%keyhash, $column_family, 'key');

	my $res = $self->_get_column($column_family, $column, undef, $key, $consistencylevel);
	my $data = $res->{column} || $res->{counter_column};

	my $value = $self->_unpack_value(
		name => [$res->{column}->{name}],
		packedstr => $data->{value},
		columnfamily => $column_family,
		mode => 'value_validation'
	);

	return $value;
}

#####################################################################################################
# get_super() allows you to pull out a key/column pair from cassandra. The client will deserialize the data
# and will return a hash containing the key, column, name, and value
#
# $obj->get_super(
#       'columnfamily'  => 'myCF', #optional if provided in object creation
#       'keyspace'      => 'myKeyspace', #optional if provided in object creation
#       'key'           => 'myKey',
#       'columnname'    => 'myColumn'
# );
#####################################################################################################
sub get_super() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $key = $opts{key};
	my $column = $opts{columnname};
	my $consistencylevel = $opts{consistency_level} || $self->{read_consistency_level};

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);
	my %keyhash = ('values' => [$key]);
	my $packedkey = $self->_pack_values(\%keyhash, $column_family, 'key');

	my $res = $self->_get_column($column_family, undef, $column, $key, $consistencylevel);
	my $data = $res->{column} || $res->{counter_column} || $res->{super_column};

	my $return={};
	foreach my $entry (@{$data->columns}) {
        	my $value = $self->_unpack_value(
        		name => [$entry->{name}],
			packedstr => $entry->{value},
			columnfamily => $column_family,
			mode => 'value_validation'
			);
		$return->{$entry->{name}} = $value;
	}

	return $return;
}
#####################################################################################################
# multiget_slice allows you to pull out a keys/columns from cassandra. The client will deserialize the data
# and will return a hash ref:
# { key => { column => value, }, ... }
#
# $obj->multiget_slice(
#	'columnfamily'  => 'myCF', #optional if provided in object creation
#	'keyspace'	=> 'myKeyspace', #optional if provided in object creation
#	'keys'		=> ['key', ...],
#	'columnnames'	=> ['column' , ...]
# );
#
# $obj->multiget_slice(
#	'columnfamily'  => 'myCF', #optional if provided in object creation
#	'keyspace'	=> 'myKeyspace', #optional if provided in object creation
#	'keys'		=> ['key', ...],
#	'columnstart'	=> 'abc',
#	'columnfinish'	=> 'zxy',
#	'reversed'	=> 0, #optional, defaults to 0, specify 1 to reverse results
#	'count'		=> 100 #optional, defaults to 100, specify number of columns to return in query
# );
#####################################################################################################
sub multiget_slice {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $consistencylevel = $opts{consistency_level} || $self->{read_consistency_level};
	my $columns = $opts{columnnames};

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);

	my @keys = @{ $opts{keys} // [] };
	my @keys_packed;
	foreach(@keys) {
        	push @keys_packed, $self->_pack_values({values => [$_]}, $column_family, 'key');
	}

	# pack the column names unless object created specifying we shouldn't pack the values
	if ($self->{donotpack} == 1) {
		unless (ref($columns) eq "HASH") {
			# deal with a scalar being passed instead of an array
			my %columnnamehash = ('values' => [$columns]);
			$columns = $self->_pack_values(\%columnnamehash, $column_family, 'column');
		} else {
			$columns = $self->_pack_values($columns, $column_family, 'column');
		}
	}

	my $column_parent = new Cassandra::ColumnPath();
	$column_parent->{column_family} = $column_family;
	$column_parent->{column} = $columns;

	my $slice_range = new Cassandra::SliceRange();
	$slice_range->{start} = $opts{columnstart};
	$slice_range->{finish} = $opts{columnfinish};
	if (defined($opts{reversed}) && $opts{reversed} == 1) {
		$slice_range->{reversed} = 1;
	}
	
	$slice_range->{count} = $opts{count} // 100;

	# you can either have a slice_range OR an array of column_names
	if (defined($opts{columnstart}) && defined($opts{columnnames})) {
		die('multiget_slice can either have a column range specified (start / finish) OR an array of column names');
	}

	my $predicate;
	if (defined($opts{columnstart}) && defined($opts{columnfinish})) {
		$predicate = Cassandra::SlicePredicate->new({
			slice_range     => $slice_range
		});
	} elsif (defined(@{$opts{columnnames}}[0])) {
		$predicate = Cassandra::SlicePredicate->new({
			column_names	=> $opts{columnnames}
		});
	} else {
		die('multiget_slice must be called with either an array of columnnames OR a start and finish column name'); 
	}


	my $res = $self->_do_multiget_slice_with_timeout(sub {
		$self->{client}->multiget_slice(
		\@keys,
		$column_parent,
		$predicate,
		$consistencylevel,
		);
	});

	my $r = {};
	while(my($k,$v) = each %{ $res // {} }) {
		my %cols;
		foreach(@{ $v // [] }) {
			my $data = $_->{column} || $_->{counter_column};
			$cols{$data->{name}} = $self->_unpack_value(
				name => [$data->{name}],
				packedstr => $data->{value},
				columnfamily => $column_family,
				mode => 'value_validation'
			);
		}
		$r->{$k}  = \%cols;
	}

	return $r;
}

sub _do_multiget_slice_with_timeout {
	my ($self, $func, $args)    = @_;

	if (defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Get Timed Out"; };
		my $alarm = alarm($self->{timeout});
		&$func(@{ $args // [] });
		alarm($alarm);
	} else {
		&$func(@{ $args // [] });
	}
}

#####################################################################################################
# _pack_values() takes a string and packs the values as determined by the ComparatorType on the columnfamily
# The ComparatorType is pulled from Cassandra by default, future versions will let you manually specify
# the validation classes. $name should be the columnname and only needs to be passed when you are packing a value
#
# my %composite = (
# 	'values'     => ['test1', 0, 'bytes2', time],
#	'column_comparators' => ['UTF8Type','IntegerType','UTF8Type','LongType'] # optional, manually specify validators on a per pack basis
# );
#
# _pack_values(\%composite, 'mycf');
#####################################################################################################
sub _pack_values() {
	my ($self, $composite, $columnfamily, $type, $columnname) = @_;
	my %composite = %$composite;

	# if metadata was specified on the columnfamily we need to check if the column being packed
	# has a different validation class specified than the default validation class
	my %metadata_validators = %{$self->{metadata_validation}{$columnfamily}};

	# if a array of validation classes has been passed in with the name hash, use that, 
	# otherwise, determine it from the keyspace definition
	my @validationComparators;	
	if (defined($type) && $type eq 'key') {
		@validationComparators = @{$self->{key_validation}{$columnfamily}};
	} elsif (defined($type) && $type eq 'value') {
		# this is a hack to make sure we use the right validation type for now for metadata 
		# columns with specific values
		if (defined($columnname) && defined($metadata_validators{$columnname})) {
			@validationComparators = $metadata_validators{$columnname};
		} else {
		# otherwise just use the default value validator for this column family if a specific
		# value validator for this column name hasn't been passed
			@validationComparators = @{$self->{value_validation}{$columnfamily}};
		}
	} else {
		# use validators provided in composite hash if defined
		if (defined($composite{column_comparators}) && scalar(@{$composite{column_comparators}}) > 0) {
			@validationComparators = @{$composite{column_comparatorss}};
		} else {
			@validationComparators = @{$self->{comparators}{$columnfamily}};
		}
	}


	# if this is a query string the logic is a bit different
	if (defined($composite{start}) && defined($composite{finish})) {
		my ($startslice, $finishslice);
		
		my @startpackoptions;
		my @startpackvalues;
		my @finishpackoptions;
		my @finishpackvalues;

		# check if we are given a composite for a start or finish that we were given at least the number given of validators
		if (scalar(@{$composite{'start'}}) > scalar(@validationComparators)) {
			die("[ERROR] More start components were given than validators defined for column family [$columnfamily]");
		}

		if (scalar(@{$composite{'finish'}}) > scalar(@validationComparators)) {
			die("[ERROR] More finish components were given than validators defined for column family [$columnfamily]");
		}

		#first take care of the start query pack
		my $i = 0;
		foreach my $val (@{$composite{'start'}}) {
			#pack nothing if '' specified
			last if (scalar(@{$composite{'start'}}) == 1 && ${$composite{'start'}}[0] eq '');

			my $packstring = $val;

			unless ($packstring eq '') {
				$packstring = pack($validation_map{@{$self->{comparators}{$columnfamily}}[$i]}, $val);
			}

			my $length = length($packstring);

			push(@startpackoptions, 'n');
			push(@startpackvalues, $length);

			push(@startpackoptions, "a$length");
			push(@startpackvalues, $packstring);

			push(@startpackoptions, 'c');

			# each element should be seperated by a 0, otherwise terminate the query
			# as either a LESS_THAN_EQUAL => -1, EQUAL => 0, or GREATER_THAN_EQUAL => 1
			#
			# if not specified by user, default to EQUAL for start
			if (${$composite{'start'}}[-1] eq $val) {
				if (defined($composite{'start_equality'}) && lc($composite{'start_equality'}) eq 'less_than_equal') {
					push(@startpackvalues, -1); #LESS_THAN_EQUAL (e.g. non-inclusive)
				} elsif (defined($composite{'start_equality'}) && lc($composite{'start_equality'}) eq 'greater_than_equal') {
					push(@startpackvalues, 1); #GREATER_THAN_EQUAL
				} else {
					push(@startpackvalues, 0); #EQUAL (default)
				}
			} else {
				push(@startpackvalues, 0);
			}
			
			$i++;
		}

		#next take care of the finish query (is the search inclusive?)
		my $j = 0;
		$i = 0;
		foreach my $val (@{$composite{'finish'}}) {
			#pack nothing if '' specified
			last if (scalar(@{$composite{'finish'}}) == 1 && ${$composite{'finish'}}[0] eq '');

			my $packstring = $val;

			unless ($val eq '') {
				$packstring = pack($validation_map{@{$self->{comparators}{$columnfamily}}[$i]}, $packstring);
			}

			my $length = length($packstring);

			push(@finishpackoptions, 'n');
			push(@finishpackvalues, $length);

			push(@finishpackoptions, "a$length");
			push(@finishpackvalues, $packstring);

			push(@finishpackoptions, 'c');

			if (${$composite{'finish'}}[-1] eq $val) {
				if (defined($composite{'finish_equality'}) && lc($composite{'finish_equality'}) eq 'less_than_equal') {
					push(@finishpackvalues, -1); #LESS_THAN_EQUAL (e.g. non-inclusive)
				} elsif (defined($composite{'finish_equality'}) && lc($composite{'finish_equality'}) eq 'equal') {
					push(@finishpackvalues, 0); #EQUAL
				} else {
					push(@finishpackvalues, 1); #GREATER_THAN_EQUAL (default)
				}
			} else {
				push(@finishpackvalues, 0);
			}

			$i++;
			$j++;
		}

		if ($self->{debug} == 1) {
			print STDERR "[DEBUG] Pack for start query will be @startpackoptions  |  @startpackvalues\n";
			print STDERR "[DEBUG] Pack for finish query will be @finishpackoptions | @finishpackvalues\n";
		}

		my $startquery = pack(join(' ', @startpackoptions),@startpackvalues); 
		my $finishquery = pack(join(' ', @finishpackoptions),@finishpackvalues);

		if ($self->{debug} == 1) {
			print STDERR "[DEBUG] HEX String for start query is ".join(" ", map( { sprintf("%02x", ord($_)); } unpack("(a1)*", $startquery))) . "\n";
			print STDERR "[DEBUG] HEX String for finish query is ".join(" ", map( { sprintf("%02x", ord($_)); } unpack("(a1)*", $finishquery))) . "\n";
		}

		return (\$startquery, \$finishquery);
	}


	# TODO: validate the passed in compositetype values to make sure they are valid validators that we know about

	# if we got this far assume we want to pack values for an insert and not a query
	#counter so we know where we are in the array
	my @packoptions;
	my @packvalues;
	my $i = 0;


	foreach my $validationtype (@validationComparators) {
		my $value = @{$composite{'values'}}[$i];

		# bc we need to support dynamic columns check if we have less values than comparator types
		if($i >= scalar(@{$composite{'values'}})) {
			next;
		}


		$i++; #incremenet our counter for the next item in the array after this pack operation succeeds

		# check what the compositetype validator is supposed to be and pack accordingly
		if ( grep /^$validationtype$/i, ('UTF8Type', 'utf8')) {
			my $utf8value;

			eval {
				$utf8value = encode('utf8', $value);
			};

			if($@) {
				die('could not encode string[$key] into utf8');
			}

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'a*');
				push(@packvalues, $utf8value);
			} else {
				push(@packoptions, 'n');
				push(@packoptions, 'a*');
				push(@packoptions, 'C');
				
				my $length = length($utf8value);
				push(@packvalues, $length);
				push(@packvalues, $utf8value);
				push(@packvalues, 0);
			}

		} elsif( grep /^$validationtype$/i, ('AsciiType')) {
			#check if we have non-ascii characters, if so die
			if ($value =~ /[[:^ascii:]]/ ) {
				die('there were non ascii characters in string [$key]');
			}

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'A*');
				push(@packvalues, "$value");
			} else {
				push(@packoptions, 'n');
				push(@packoptions, 'A*');
				push(@packoptions, 'C');

				my $length = length($value);
				push(@packvalues, $length);
				push(@packvalues, "$value");
				push(@packvalues, 0);
			}
			
		} elsif( grep /^$validationtype$/i, ('BooleanType')) {
			unless ($value == 1 || $value == 0) {
				die('you must specify either TRUE (1) or FALSE (0) for a BooleanType validator');
			}

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'C');
				push(@packvalues, $value);
			} else {
				push(@packoptions, 'n');
				push(@packoptions, 'C');
				push(@packoptions, 'C');

				my $length = length($value);
				push(@packvalues, $length);
				push(@packvalues, $value);
				push(@packvalues, 0);
			}

		} elsif(grep /^$validationtype$/i, ('BytesType')) {
			# if this is bytestype and not composite, do not pack with length etc
			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'a*');
				push(@packvalues, $value);	
			} else {	
				push(@packoptions, 'n');
				push(@packoptions, 'a*');
				push(@packoptions, 'C');

				my $length = length($value);
				push(@packvalues, $length);
				push(@packvalues, $value);
				push(@packvalues, 0);
			}

		} elsif(grep /^$validationtype$/i, ('FloatType', 'float')) {
			my $tmpfloat = pack('f', $value);

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'a*');
				push(@packvalues, $tmpfloat);
			} else {
				push(@packoptions, 'n');
				push(@packoptions, 'a*');
				push(@packoptions, 'C');

				my $length = length($tmpfloat);
				push(@packvalues, $length);
				push(@packvalues, $tmpfloat);
				push(@packvalues, 0);
			}

		} elsif(grep /^$validationtype$/i, ('Int32Type')) {
			if ($value =~ /\D/) {
				die("[$value] is not a valid int");
			}

			# assume we only ever care about what can fit in 32-bits
			$value = int($value);
			my $hi = $value >> 32;

			if ($hi != 0) {
				die("[$value] does not fit inside a 32-bit int");
			}

			my $lo = $value & 0xFFFFFFFF;
			my $tmpint = pack('N', $lo);

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'a*');
				push(@packvalues, $tmpint);
			} else {
				push(@packoptions, 'n');
				push(@packoptions, 'a*');
				push(@packoptions, 'C');

				my $length = length($tmpint);
				push(@packvalues, $length);
				push(@packvalues, $tmpint);
				push(@packvalues, 0);
			}

		} elsif(grep /^$validationtype$/i, ('IntegerType', 'int', 'LongType', 'long', 'DateType', 'date', 'uuid', 'UUIDType')) {
			if ($value =~ /\D/) {
				die("[$value] is not an int");
			}

			# pack it as a 64-bit/8-byte int
			my $hi = $value >> 32;
			my $lo = $value & 0xFFFFFFFF;
			my $tmpint = pack('N2', $hi, $lo);

			if (scalar(@validationComparators) == 1) {
				push(@packoptions, 'a*');
				push(@packvalues, $tmpint);
			} else {			
				my $length = length($tmpint);
				push(@packoptions, 'n');
				push(@packoptions, 'a'.$length);
				push(@packoptions, 'C');

				push(@packvalues, $length);
				push(@packvalues, $tmpint);
				push(@packvalues, 0);

			}
		} else {
			#TODO: make this warning more useful
			use Data::Dumper;
			print Dumper(%composite);
			die("unsupported ValidationType specified [$validationtype]");
		}
	}

	return pack(join(' ', @packoptions),@packvalues);
}

#####################################################################################################
# _unpack_value() takes in a packed string and unpacks it based on the validation class on either
# the key or the value
#####################################################################################################
sub _unpack_value() {
	my ($self, %opts) = @_;

	my $columnfamily = $opts{columnfamily} || $self->{columnfamily};
	my $packedstr = $opts{packedstr};
	my $mode = $opts{mode};
	my $name = $opts{name};

	if (!defined($packedstr)) {
		die('[ERROR] The value to decode must be defined');
	}

	if (!defined($mode) || $mode !~ /(value_validation|key_validation)/) {
		die('[ERROR] mode must be defined. Value can be either \'value_validation\' or \'key_validation\'');
	}
	
	# we should have either had the validation class manually passed in or at least been able to get it directly from
	# the cluster. if we failed at both of these we don't know how to unpack the given value so die()
	if (!@{$self->{$mode}{$columnfamily}}) {
		die('[ERROR] Was unable to retrieve the validation class for column family $columnfamily. Unable to unpack\n');
	}

	my $meta_sourced_validation_class;
	# check if the name of this column has metadata and a validator
	if (defined($self->{metadata_validation}{$columnfamily}) && defined(@{$name}[0])) {
		my $mv_hashref = $self->{metadata_validation}{$columnfamily};
		my %meta = %$mv_hashref;
		if (defined($meta{@{$name}[0]})) {
			$meta_sourced_validation_class = $meta{@{$name}[0]};
		}
	}

	my $unpackedstr;
	if (defined($meta_sourced_validation_class) && $meta_sourced_validation_class eq 'IntegerType') {
		my $hexstr = '0x';
		$hexstr .= join("", map( { sprintf("%02x", ord($_)); } unpack("(a1)*", $packedstr)));
		my $dec = Math::BigInt->new($hexstr);

		for (my $i = 0; $i <= scalar(@{$dec->{value}}); $i++) {
			$unpackedstr .= pop(@{$dec->{value}});
		}  
	} elsif (defined($meta_sourced_validation_class) && $meta_sourced_validation_class ne 'IntegerType') {
		$unpackedstr = unpack($validation_map{$meta_sourced_validation_class}, $packedstr);
	} else {
		foreach my $validator (@{$self->{$mode}{$columnfamily}}) {
			$unpackedstr = unpack($validation_map{$validator}, $packedstr);
		}
	}
	
	return $unpackedstr;
}

#####################################################################################################
# _unpack_columnname_values() takes the values returned in the columnname and unpacks them according to the CF
# comparatortype
#####################################################################################################
sub _unpack_columnname_values() {
	my ($self, $composite, $columnfamily) = @_;

	if (!defined($columnfamily)) {
		$columnfamily = $self->{columnfamily};
	}

	# if the columnname isn't composite, just return the string
	# this will skip the rest of the processing
	if (scalar(@{$self->{comparators}{$columnfamily}}) == 1) {
		return $composite;
	}

	if ($self->{debug} == 1) {
		# print out the hex dump of composite value
		print STDERR "[DEBUG] ". join(" ", map({sprintf("%02x", ord($_)); } unpack("(a1)*",$composite))) . "\n";
	}

	my $unpackstr = 'n';
	my @ret = ();
	my $term = 0;
	while (!$term) {
		@ret = unpack($unpackstr, $composite);
		my $chars = $ret[-1]; # get the length of the first packed item
		$unpackstr .= "a".$chars."W";
		@ret = unpack($unpackstr, $composite);

		# a composite should be terminated with a 1 as a delimited, otherwise its a null padded character (0)  and we continue
		$term = $ret[-1];
	
		# assume because this is a composite key that when the length is 0 we reached the end of the string
		if ($chars == 0) {
			$term = 1;
		} else {
			$unpackstr .= "n";
		}
	}

	# now that we have the template to give to pack for this string, unpack the actual string into an array
	my @temp = unpack($unpackstr, $composite);
	my @deserialized = ();

	# now let's pull out the second element of each 3 element pair from the unpack
	my $length = scalar(@temp) - 1;
	my $r = 0;
	for (my $t = 0; $t < $length/3; $t++) {
		for (my $q = 0; $q <= 2; $q++) {
			# we only want the 2nd element (the pack type) and only do it if we actually have a packed value
			if ($q == 1 && defined(@{$self->{comparators}{$columnfamily}}[$t])) {
				# now that we have the packed value deserialized, unpack once again based on validation class
				# once we get the deserialized and unpacked value -> shove it into an array to be returned
				my $deserializedval;
				if (@{$self->{comparators}{$columnfamily}}[$t] eq 'DateType' || @{$self->{comparators}{$columnfamily}}[$t] eq 'IntegerType' || @{$self->{comparators}{$columnfamily}}[$t] eq 'LongType') {
					my @unpacked = unpack($validation_map{@{$self->{comparators}{$columnfamily}}[$t]}, $temp[$r]);
					$deserializedval = ($unpacked[0] << 32) | $unpacked[1];
				} else {
					$deserializedval = unpack($validation_map{@{$self->{comparators}{$columnfamily}}[$t]}, $temp[$r]);
				}

				push (@deserialized, $deserializedval);
			}
			$r++;
		}
	}

	return @deserialized;

}

#####################################################################################################
# get_slice() returns a ref to an array of results based on a 'start' and 'finish' query
#
# my $results = $obj->get_slice(
#	'columnfamily' 	=> 'myCF', #optional if specified in object creation
#	'keyspace'	=> 'myKeyspace', #optional if specified in object creation
#	'key'		=> 'myKey',
#	'start'		=> ['part1', 'part2'],
#	'finish'	=> ['part1', 'part4']
# );
#
# foreach my $res (@$results) {
#	my @name = @{$res->{name}};
#	my $value = $res->{value};
# }
#####################################################################################################
sub get_slice() {
	my ($self, %opts) = @_;

	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $key = $opts{key} || undef;
	my $consistencylevel = $opts{consistency} || $self->{read_consistency_level};

	my %query;
	if (defined($opts{start}) && defined($opts{finish}) && scalar(@{$opts{start}}) > 0) {
		%query = (
			'start' 		=> \@{$opts{start}},
			'finish'		=> \@{$opts{finish}},
			'start_equality'	=> $opts{start_equality},
			'finish_equality'	=> $opts{finish_equality}
		);
	} else {
		die('you must provide both \'start\' and \'finish\' arrays as hash elements');
	}

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);
	my $client = $self->{client};
	
	# pack the requests first
	my ($slicestart, $slicefinish) = $self->_pack_values(\%query, $column_family);

	my $column_parent = new Cassandra::ColumnParent({column_family => $column_family});
	my $slice_range = new Cassandra::SliceRange();
	$slice_range->{start} = $$slicestart;
	$slice_range->{finish} = $$slicefinish;
	my $predicate = new Cassandra::SlicePredicate();
	$predicate->{slice_range} = $slice_range;

	my $res;
	if(defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Get Slice Timed Out"; };
		alarm($self->{timeout});
		$res = $client->get_slice($key, $column_parent, $predicate, $consistencylevel);
		alarm($self->{timeout});
	} else {
		$res = $client->get_slice($key, $column_parent, $predicate, $consistencylevel);
	}

	close_conn();

	return $self->_deserialize_column_array($res, $opts{return_expired});
}

sub _deserialize_column_array() {
	my ($self, $serialized_array, $return_expired) = @_;

	my @return;
	foreach my $result (@{$serialized_array}) {
		# remove expired columns from returned results unless overridden by 'return_expired' in opts:
		if (defined($result->{column}{timestamp}) && defined($result->{column}{ttl})) {
			my $expiretime = $result->{column}{timestamp} + $result->{column}{ttl};
            # XXX TODO may be a bug to compare timestamp with ttl. ttl in seconds, time stamp in milliseconds?
			if ($expiretime < time && $return_expired != 1) {
				next;
			}
		}

		# if we got back a record without a timestamp something is wrong, le's just skip it by default
		next if (!defined($result->{column}{timestamp}));

		my @names = $self->_unpack_columnname_values($result->{column}->{name});
		my $value = $self->_unpack_value(packedstr => $result->{column}->{value}, mode => 'value_validation', name => \@names);

		my %resvalue = ( 
			value => $value,
			name => \@names,
			timestamp => $result->{column}{timestamp},
			ttl => $result->{column}{ttl}
		);

		push (@return, \%resvalue);
	}

	return \@return;
	
}

sub get_range_slices() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $consistencylevel = $opts{consistency} || $self->{read_consistency_level};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $key_start = $opts{key_start} || '';
	my $key_finish = $opts{key_finish} || '';
	my $column_max_count = $opts{column_max_count} || 100;
	my $key_max_count = $opts{key_max_count} || 100;
	my $reversed = $opts{reversed} || 0;
	my $buffer_size = $opts{buffer_size} || 1024;

	my %return; 

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);
	my $client = $self->{client};

	my @emptyarray = [''];

	my %query = (
		'start' 		=> \@{$opts{column_start}} || \@emptyarray,
		'finish'		=> \@{$opts{column_finish}} || \@emptyarray,
		'start_equality'	=> $opts{start_equality},
		'finish_equality'	=> $opts{finish_equality}
	);

	# pack the column names first
	my ($column_slicestart, $column_slicefinish) = $self->_pack_values(\%query, $column_family);

	# we need to make multiple requests if there are more records requested than what can fit in the buffer
	if ($key_max_count > $buffer_size || $column_max_count > $buffer_size) {
		my $numtorun_key = $key_max_count/$buffer_size;
		my $numtorun_col = $column_max_count/$buffer_size;
		# make sure we round up so we run enough times
		$numtorun_key = int($numtorun_key) + ($numtorun_key != int($numtorun_key));
		$numtorun_col = int($numtorun_col) + ($numtorun_col != int($numtorun_col));

		my $runcount_key = 0;
		my $finished = 0;

		my $tmpstart_key;
		my $tmpfinish_key;

		# first how many times to we need to loop over the keys first, then deal with buffering columns
		while ($finished != 1 && $runcount_key < $numtorun_key) {
			if (!defined($tmpstart_key)) {
				$tmpstart_key = $key_start;
			}

			if (!defined($tmpfinish_key)) {
				$tmpfinish_key = $key_finish;
			}

			# if the user provided a column_max_count smaller than buffer size, use that
			my $tmp_column_max_count;
			if ($column_max_count < $buffer_size) {
				$tmp_column_max_count = $column_max_count;
			} else {
				$tmp_column_max_count = $buffer_size; 
			}

			# we need to page the columns
			my $sliceres;
			if ($column_max_count > 1024) {
				#call one key at a time 
				$sliceres = $self->_call_get_range_slices(
					keyspace	=> $keyspace,
					column_family 	=> $column_family,
					column_start	=> $$column_slicestart,
					column_end	=> $$column_slicefinish,
					key_start	=> $tmpstart_key,
					key_end		=> $tmpfinish_key,
					column_max_count=> $tmp_column_max_count,
					key_max_count	=> $key_max_count,
					reversed	=> $reversed,
					consistencylevel=> $consistencylevel,
					order		=> \@{$return{'**order**'}}
				);

				#buffer $buffer_size keys at a time the columns
				foreach my $key (keys(%$sliceres)) {
					next if $key eq '**order**';
					my %tt = %$sliceres;

					my $colbuffinished = 0;

					while ($colbuffinished == 0) {
						my $lastcol = pop(@{$tt{$key}});
						my %lastcol_hash = %$lastcol;
						my $lastcol_name = $lastcol_hash{name}[0];
						
						my $sub_sliceres = $self->_call_get_range_slices(
							keyspace 	=> $keyspace,
							column_family	=> $column_family,
							column_start	=> $lastcol_name,
							column_end	=> '',
							key_start	=> $key,
							key_end		=> $key,
							column_max_count=> $buffer_size,
							key_max_count 	=> $key_max_count,
							reversed	=> $reversed,
							consistencylevel=> $consistencylevel,
							order		=> \@{$return{'**order**'}}
						);

						my %temp_tt = %$sub_sliceres;
						foreach my $col (@{$temp_tt{$key}}) {
							push @{$tt{$key}}, $col; 
						}

						if ($self->{debug} == 1) {
							print STDERR "DEBUG: buffered $buffer_size keys for a multiget_slice\n";
						}

						if (scalar(@{$temp_tt{$key}}) < 1024 || scalar(@{$tt{$key}}) >= $column_max_count) {
							$colbuffinished = 1;
						}
					}

				}
			} else {
				#make a request from $tmpmin_key up to the buffer
				$sliceres = $self->_call_get_range_slices(
					keyspace	=> $keyspace,
					column_family 	=> $column_family,
					column_start	=> $$column_slicestart,
					column_end	=> $$column_slicefinish,
					key_start	=> $tmpstart_key,
					key_end		=> $tmpfinish_key,
					column_max_count=> $column_max_count,
					key_max_count	=> $buffer_size,
					reversed	=> $reversed,
					consistencylevel=> $consistencylevel,
					order		=> \@{$return{'**order**'}}
				);
			}

			my %tt = %$sliceres;
			%return = (%return, %tt);

			# get the last key returned and set that equal to the new start key
			my $totalkeys = scalar(keys(%$sliceres));

			if ($totalkeys < $buffer_size) {
				$finished = 1;
			} else {
				$tmpstart_key = pop(@{$tt{'**order**'}});
			}
			
			$runcount_key++;
		}
		
	} else {
		my $sliceres = $self->_call_get_range_slices(
			keyspace	=> $keyspace,
			column_family 	=> $column_family,
			column_start	=> $$column_slicestart,
			column_end	=> $$column_slicefinish,
			key_start	=> $key_start,
			key_end		=> $key_finish,
			column_max_count=> $column_max_count,
			key_max_count	=> $key_max_count,
			reversed	=> $reversed,
			consistencylevel=> $consistencylevel
		);

		my %tt = %$sliceres;
		%return = (%return, %tt);
	}

	return \%return;
}

sub _call_get_range_slices() {
	my ($self, %opts) = @_;

	my $client = $self->{client};
	my @order;
	if (defined(@{$opts{order}}[0])) {
		@order = @{$opts{order}};
	} 

	my $column_parent = new Cassandra::ColumnParent({column_family => $opts{column_family}});
	my $slice_range = new Cassandra::SliceRange();
	$slice_range->{start} = $opts{column_start};
	$slice_range->{finish} = $opts{column_end};
	$slice_range->{reversed} = $opts{reversed};
	$slice_range->{count} = $opts{column_max_count};
	my $predicate = new Cassandra::SlicePredicate();
	$predicate->{slice_range} = $slice_range;

	my $key_range = new Cassandra::KeyRange();
	$key_range->{start_key} = $opts{key_start};
	$key_range->{end_key} = $opts{key_end};
	$key_range->{count} = $opts{key_max_count};

	my $res;
	if(defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Get Range Slices Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$res = $client->get_range_slices($column_parent, $predicate, $key_range, $opts{consistencylevel});
		alarm($alarm);
	} else {
		$res = $client->get_range_slices($column_parent, $predicate, $key_range, $opts{consistencylevel});
	}

	my %deserialized;

	#make sure we merge order array from previous runs
	@{$deserialized{'**order**'}} = @order;

	my $i = 0;
	foreach my $row (@{$res}) {
		my $keyname = ${@{$res}[$i]}{'key'};
		foreach my $col (${@{$res}[$i]}{'columns'}) {
			my $deserialized_array_res = $self->_deserialize_column_array($col);
			$deserialized{$keyname} = $deserialized_array_res;
		}

		# push this key name onto array so we can track order returned to us from cassandra
		push (@{$deserialized{'**order**'}}, $keyname);

		$i++;
	}

	return \%deserialized;
}

# This logic will not preserve the order the keys were returned in from Cassandra 
sub get_paged_slice() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $consistencylevel = $opts{consistency} || $self->{read_consistency_level};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $buffer = $opts{buffer} || 1024;
	my $maxcount = $opts{max_count} || 100;
	my $starting_column = $opts{starting_column} || '';

	$self->client_setup('keyspace' => $opts{keyspace}, 'columnfamily' => $opts{column_family});
	my $client = $self->{client};

	my $tmpcol_name = $starting_column;
	my $finished = 0; # use this to bail out of our loop if we detect we reach the end of the results

	my %combined_return;
	
	while ($finished != 1) {
		my $key_range = new Cassandra::KeyRange();
		$key_range->{start_key} = '';
		$key_range->{end_key} = '';

		if ($maxcount > $buffer) {
			$key_range->{count} = $buffer;
		} else {
			$key_range->{count} = $maxcount;
		}

		my $res;
		if (defined($self->{timeout})) {
			local $SIG{ALRM} = sub { die "Get Paged Slice Timed Out"; };
			my $alarm = alarm($self->{timeout});
			$res = $client->get_paged_slice($column_family, $key_range, $tmpcol_name, $self->{consistencylevel});
			alarm($alarm);
		} else {
			$res = $client->get_paged_slice($column_family, $key_range, $tmpcol_name, $self->{consistencylevel});
		}

		foreach my $key (@{$res}) {
			my $keyname = $key->{key};
			my @columns = $key->{columns};

			my $counter = 0;
			foreach my $col (@columns) {
				foreach my $realcol (@{$col}) {
					my @column = $realcol->{column};
					push (@{$combined_return{$keyname}}, @column);
					$counter++;
				}
			}

			# now we need to grab the last one we got and start from there
			my $lastcol = pop @{$combined_return{$keyname}};
			$tmpcol_name = $lastcol->{name};

			# if we got back less results than max we've reached the end
			if ($counter < $maxcount || scalar@{$combined_return{$keyname}} => $maxcount) {
				$finished = 1;
			}
		}
	}

	return \%combined_return; 
}

sub get_column_count() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $key = $opts{key};
	my $consistencylevel = $opts{consistency} || $self->{read_consistency_level};
	my $column_start = $opts{column_start} || '';
	my $column_end = $opts{column_end} || '';
	my $column_max_count = $opts{column_max_count} || 100;
	my $reversed = $opts{reversed} || 0;

	if (!defined($key)) {
		die('[ERROR] you must provide a key to get_count()');
	}

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);
	my $client = $self->{client};

	my $column_parent = new Cassandra::ColumnParent({column_family => $column_family});

	my $slice_range = new Cassandra::SliceRange();
	$slice_range->{start} = $column_start;
	$slice_range->{finish} = $column_end;
	$slice_range->{reversed} = $reversed;
	$slice_range->{count} = $column_max_count;
	my $predicate = new Cassandra::SlicePredicate();
	$predicate->{slice_range} = $slice_range;

	my $count;
	if(defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Get Count Timed Out"; };
		my $alarm = alarm($self->{timeout});
		$count = $client->get_count($key, $column_parent, $predicate, $consistencylevel);
		alarm($alarm);
	} else {
		$count = $client->get_count($key, $column_parent, $predicate, $consistencylevel);
	}

	return $count;  
}

sub get_multicolumn_count() {
	my ($self, %opts) = @_;

	my $column_family = $opts{columnfamily} || $self->{columnfamily};
	my $keyspace = $opts{keyspace} || $self->{keyspace};
	my $keys = $opts{keys};
	my $consistencylevel = $opts{consistency} || $self->{read_consistency_level};
	my $column_start = $opts{column_start} || '';
	my $column_end = $opts{column_end} || '';
	my $column_max_count = $opts{column_max_count} || 100;
	my $reversed = $opts{reversed} || 0;

	if (!defined($keys)) {
		die('[ERROR] you must provide an array of keys to get_multicolumn_count()');
	}

	$self->client_setup('keyspace' => $keyspace, 'columnfamily' => $column_family);
	my $client = $self->{client};

	my $column_parent = new Cassandra::ColumnParent({column_family => $column_family});

	my $slice_range = new Cassandra::SliceRange();
	$slice_range->{start} = $column_start;
	$slice_range->{finish} = $column_end;
	$slice_range->{reversed} = $reversed;
	$slice_range->{count} = $column_max_count;
	my $predicate = new Cassandra::SlicePredicate();
	$predicate->{slice_range} = $slice_range;

	my @count;

	if(defined($self->{timeout})) {
		local $SIG{ALRM} = sub { die "Multiget Count Timed Out"; };
		my $alarm = alarm($self->{timeout});
		@count = $client->multiget_count($keys, $column_parent, $predicate, $consistencylevel);
		alarm($alarm);
	} else {
		@count = $client->multiget_count($keys, $column_parent, $predicate, $consistencylevel);
	}

	my %key_counts;
	foreach my $hash (@count) {
		foreach my $key (keys(%$hash)) {
			my %h = %$hash;
			$key_counts{$key} = $h{$key};
		}
	}

	return %key_counts; 
}

#####################################################################################################
# sub finish() can be called to free the cassandra server from that object and to clean up after itself
#
# $obj->finish();
#####################################################################################################
sub finish() {
	my ($self) = @_;
	
	$self->close_conn();
}


1;
