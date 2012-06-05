package perlcassa::Client;

use strict;
use warnings;
use base 'Exporter';

our @EXPORT = qw(setup close_conn client_setup);

use Thrift;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::XS::BinaryProtocol;

use ResourcePool;
use ResourcePool::Factory;
use ResourcePool::LoadBalancer;

sub get_server() {
	my $self = shift;

	if (!defined($self->{loadbalancer})) {
		$self->perlcassa::Client::_load_loadbalancer_hosts();
	}

	my $resource = $self->{loadbalancer}->get();

	if (defined($resource)) {
		$self->{server} = $resource;

		if ($self->{debug} == 1) {
			print STDERR "[DEBUG] Using server [$self->{server}->{ARGUMENT}]\n";
		}

		return $self->{server};
	} else {
		die('[ERROR] Unable to connect to any of the provided Cassandra hosts.');
	}
}

#####################################################################################################
# _load_loadbalancer_hosts() creates a new ResourcePool::LoadBalancer object with the specified hosts
#####################################################################################################
sub _load_loadbalancer_hosts() {
	my $self = shift;

	$self->{loadbalancer} = ResourcePool::LoadBalancer->new("Cassandra", Policy => "LeastUsage", "MaxTry" => 2);

	foreach my $host (@{$self->{hosts}}) {
		my $factory = ResourcePool::Factory->new($host);
		my $pool = ResourcePool->new($factory);

		$self->{loadbalancer}->add_pool($pool);
	}

}

#####################################################################################################
# close_conn() frees the Cassandra node in the resource pool and gracefully closes the thrift client
#####################################################################################################
sub close_conn() {
	my $self = shift;

	$self->{transport}->close();
	$self->{loadbalancer}->free($self->{server});
	$self->{server} = undef;
	$self->{client} = undef;

	return 1;
	
}

#####################################################################################################
# setup() is called to create a new Thrift Client. You should not need to call this manually
##################################################################################################### 
sub setup() {
	my ($self, $keyspace) = @_;

	if(!defined($self->{server})) {
		my $connected = 0;
		my $attempts = 0;
		while ($connected != 1) {
			my $serverobj = $self->perlcassa::Client::get_server();
			my $serveraddr = $serverobj->{ARGUMENT};

			$self->{socket} = new Thrift::Socket($serveraddr, $self->{port});
			$self->{transport} = new Thrift::FramedTransport($self->{socket},1024,1024);
			$self->{protocol} = new Thrift::XS::BinaryProtocol($self->{transport});
			$self->{client} = new Cassandra::CassandraClient($self->{protocol});

			if ($attempts > 2) {
				die('[ERROR] Unable to connect to a host for this request');
			}

			eval {
				$self->{transport}->open();
			};

			if ($@) {
				print STDERR "[WARNING] unable to connect to host $serveraddr, trying next available host\n";

				$self->{loadbalancer}->fail($serverobj);
				$self->{server} = undef;
				$attempts++;
			} else {
				$connected = 1;
			}
		}
	}

	$self->{client}->set_keyspace($keyspace);

	# let us track what keyspace this client is connected to
	# so we can determine if we need to reconnect for future calls using the same object
	$self->{keyspace_inuse} = $keyspace;

	return $self->{client};
}

sub _refresh_cf_info() {
	my ($self, %opts) = @_;

	my $current_cf = $self->{columnfamily_inuse};
	my $current_keyspace = $opts{keyspace} || $self->{keyspace_inuse};

	if (defined($current_cf)) {
		my $validators;
		# if we have a manual hash provided in the object, use that instead of fetching it from the cluster
		if (defined($self->{validators})) {
			if ($self->{debug} == 1) {
				print STDERR "[DEBUG] using manually provided column family validation information\n";
			}

			$validators = $self->{validators};			
		} else {
			# otherwise - fetch the validators
			if ($self->{debug} == 1) {
				print STDERR "[DEBUG] refreshing column family information from Cassandra\n";
			}

			$validators = $self->get_validators(columnfamily => $current_cf, keyspace => $current_keyspace);
		}

		my %validators = %$validators;

		#refresh key validation class for this connected client
		@{$self->{key_validation}{$current_cf}} = @{$validators{key}};

		#refresh comparator type for this connected client
		@{$self->{comparators}{$current_cf}} = @{$validators{comparator}};
	 
		#refresh column value for this connected client
		@{$self->{value_validation}{$current_cf}} = @{$validators{column}};
	}
}

#####################################################################################################
# client_setup() checks to make sure the current open client for this object was created for
# the correct keyspace and columnfamily. Because all insert(), get() requests etc can be overloaded
# with 'keyspace' => 'myNewKeyspace' or 'columnfamily' => 'myNewColumnFamily' we need to make sure
# the client was created for these cf and keyspaces or Thrift/Cassandra will throw an exception
#####################################################################################################
sub client_setup() {
	my ($self, %opts) = @_;

	if (!defined($self->{columnfamily_inuse})) {
		if (defined($opts{columnfamily})) {
			$self->{columnfamily_inuse} = $opts{columnfamily};
		}
	}
	if (!defined($self->{keyspace_inuse})) {
		if (defined($opts{keyspace})) {
			$self->{keyspace_inuse} = $opts{keyspace};
		}
	}

	# check if we have already opened a client connection
	# if the user manually passed in a keyspace or column family to the insert call
	# check if the current connection is to that keyspace and column family
	# otherwise we need to disconnect and reconnect using that column family and keyspace
	if (!defined($self->{client})) {
		$self->setup($self->{keyspace});

		if (defined($opts{columnfamily})) {
			$self->{columnfamily_inuse} = $opts{columnfamily};
		}

		$self->perlcassa::Client::_refresh_cf_info(keyspace => $self->{keyspace});
		
	} elsif (defined($self->{client}) && defined($opts{keyspace}) && $opts{keyspace} ne $self->{keyspace_inuse}) {
		my $keyspace = $opts{keyspace} || $self->{keyspace};

		# just to be safe lets close the old client
		$self->close_conn();
		$self->setup($keyspace);

		if (defined($opts{columnfamily})) {
			$self->{columnfamily_inuse} = $opts{columnfamily};
		}

		if (defined($opts{keyspace})) {
			$self->{keyspace_inuse} = $keyspace;
		}

		$self->perlcassa::Client::_refresh_cf_info(keyspace => $keyspace);
	} elsif (defined($self->{client}) && defined($opts{columnfamily}) && $opts{columnfamily} ne $self->{columnfamily_inuse}) {
		my $keyspace = $opts{keyspace} || $self->{keyspace};
		$self->setup($keyspace);

		if (defined($opts{columnfamily})) {
			$self->{columnfamily_inuse} = $opts{columnfamily};
		}
		
		if (defined($opts{keyspace})) {
			$self->{keyspace_inuse} = $keyspace;
		}


		$self->perlcassa::Client::_refresh_cf_info(keyspace => $keyspace);
	} elsif (defined($self->{client}) || $opts{keyspace} eq $self->{keyspace_inuse} || $opts{columnfamily} eq $self->{columnfamily_inuse}) {
		#no need to reconnect client as it is already ready
	} else {
		$self->close_conn();
		$self->setup($opts{keyspace});

		if (defined($opts{columnfamily})) {
			$self->{columnfamily_inuse} = $opts{columnfamily};
		}

		$self->perlcassa::Client::_refresh_cf_info(keyspace => $opts{keyspace});
	}
}

1;
