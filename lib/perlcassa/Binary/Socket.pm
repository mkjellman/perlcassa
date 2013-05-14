package perlcassa::Binary::Socket;

use strict;
use warnings;

use perlcassa::Binary::Pack;
use perlcassa::Binary::Stream;

use AnyEvent;
use AnyEvent::Log;
use AnyEvent::Handle;
use AnyEvent::Socket;
use AnyEvent::Strict;

our %request_codes = (
	'STARTUP'	=> '01',
	'CREDENTIALS'	=> '04',
	'OPTIONS'	=> '05',
	'QUERY'		=> '07',
	'PREPARE'	=> '09',
	'EXECUTE'	=> '0A',
	'REGISTER'	=> '0B'
);

our %response_codes = (
	'ERROR'		=> '00',
	'READY'		=> '02',
	'AUTHENTICATE'	=> '03',
	'SUPPORTED'	=> '06',
	'RESULT'	=> '08'
);

our %event_codes = (
	'EVENT'		=> '0C'
);

our %version_codes = (
	'REQUEST'	=> '01',
	'RESPONSE'	=> '81'
);

our %flags = (
	'NONE'		=> '00',
	'COMPRESSION'	=> '01',
	'TRACING'	=> '02'
);

our %consistency_levels = (
	'ANY'		=> encode_short(0),
	'ONE'		=> encode_short(1),
	'TWO'		=> encode_short(2),
	'THREE'		=> encode_short(3),
	'QUORUM'	=> encode_short(4),
	'ALL'		=> encode_short(5),
	'LOCAL_QUORUM'	=> encode_short(6),
	'EACH_QUORUM'	=> encode_short(7)
);

sub new() {
	my ($class, %opt) = @_;

	bless my $self = {
		port => $opt{binaryport} || 9042,
		server => $opt{server} || "127.0.0.1",
		handle => undef,
		avaliablestreams => [],
		inusestreams => [],
		queue => [],
	}, $class;

	$self->connect();

	my $initstreamcount = 128;
	if(defined($opt{initalstreamobjects})) {
		$initstreamcount = $opt{initalstreamobjects};
	}

	if(defined($opt{initalstreamobjects}) && $opt{initialstreamobjects} > 128) {
		die("The initialstreamobjects cannot be greater than 128. $initstreamcount is invalid");
	}

	for(my $i = 0; $i < $initstreamcount; $i++) {
		if($i > 128) {
			die("Somehow we attempted to create more than 128 stream objects (the limit for the native protocol). Please file a bug");
		}

		my $streamobj = new perlcassa::Binary::Stream(
			id => $i,
		);

		@{$self->{avaliablestreams}}[$i] = $streamobj;
	}

	return $self;
}

sub connect() {
	my ($self) = @_;

	my $cv = AE::cv;
	$self->init_server($cv);
}

sub init_server() {
	my ($self, $cv) = @_;

	my $guard; $guard = tcp_connect $self->{server}, $self->{port}, sub {
		$self->create_socket(@_);
		$self->send_cassandra_preamble($cv);
	};
	$cv->recv;

}

sub send_cassandra_preamble {
	my ($self, $cv) = @_;

	my $handle = $self->{handle};

	my @startup_opts;
	my %b = ();
	$b{CQL_VERSION} = "3.0.1";
	push(@startup_opts, \%b);

	my $body = encode_string_map(\@startup_opts);

	$cv->begin();
	$handle->push_write(cassandra_bin => $version_codes{REQUEST},
		$flags{NONE}, 0, $request_codes{STARTUP}, $body);
	$handle->push_read(cassandra_bin => sub {
		my ($msg) = @_;
		my $opcode = $msg->{opcode};
		$cv->end();
	});

}

sub create_socket() {
	my ($self, $fh, $host, $port) = @_;

	AnyEvent::Log::ctx->log_cb(sub { print STDOUT shift; 0 });
	AnyEvent::Log::ctx->level("trace");

	if(!defined($fh)) {
		die('Unable to connect to C*');
	}

	binmode($fh);

	my $handle;
	$handle = AnyEvent::Handle->new(
		fh => $fh,
		on_drain => sub {

		},
		on_eof => sub {
			$handle->distroy;
			AE::log info => "Done."
		},
		on_error => sub {

		},
	);

	$self->{handle} = $handle;

}

AnyEvent::Handle::register_read_type cassandra_bin => sub {
	my ($self, $cb) = @_;

	my %state = ();
	sub {
		return unless $_[0]{rbuf};

		my $rbuf_ref = \$_[0]{rbuf};
		my $header = substr($$rbuf_ref, 0, 8, ''); #get the first 8 bytes for the frame header
		my $response = unpack("H*", $header);

		$state{version} = hex(substr($response, 0, 2));
		$state{flags} = hex(substr($response, 2, 2));
		$state{stream} = hex(substr($response, 4, 2));
		$state{opcode} = substr($response, 6, 2);
		$state{length} = hex(substr($response, 8, 8));

		unless($state{length} == 0) {
			$state{body} = substr($$rbuf_ref, 0, $state{length}, '');
		}

		$cb->(\%state);
		undef %state;
		1;
	}	
};

AnyEvent::Handle::register_write_type cassandra_bin => sub {
	my ($self, $version, $flags, $stream, $opcode, $body) = @_;

	$version = pack('C', $version);
	$flags = pack('C', $flags);
	$stream = pack('C', $stream);
	$opcode = pack('C', $opcode);

	my $bodylen = perlcassa::Binary::Pack::encode_int(length($body));
	my $req = $version.$flags.$stream.$opcode.$bodylen.$body;
	return $req;
};

sub submit($) {
	my ($self) = @_;

	my $cv = AnyEvent->condvar;

	#my $w = AnyEvent->idle(
	#	cb => sub {
	#	}
	#)

	my $w2 = AnyEvent->timer(
		after => 0,
		interval => 1,
		cb => sub {
			#my $obj = pop($self->{queue});
			use Data::Dumper;
			if(scalar(@{$self->{queue}}) == 0) {
				#print "***finished with queue\n";
				$cv->send;
			} else {
				#pull off up to 128 from the queue
				if(scalar(@{$self->{avaliablestreams}}) > 0) {
					my $elmtodequeue = 0;
					if(scalar(@{$self->{queue}}) >= 128) {
						$elmtodequeue = 128;
					} else {
						$elmtodequeue = scalar(@{$self->{queue}});
					}

					for(my $i = 0; $i < $elmtodequeue; $i++) {
						my $query = shift(@{$self->{queue}});
						$self->_query($query);
					}
				}
			}
		}
	);
	$cv->recv;
}

sub query($) {
	my ($self, $query, $consistencylevel, $cb) = @_;

	my %obj = ();
	$obj{query} = $query;
	$obj{consistencylevel} = $consistencylevel;
	$obj{cb} = $cb;

	if(scalar(@{$self->{avaliablestreams}}) == 0) {
		#force flush if we have 128 pending elements in the queue
		$self->submit();
		push(@{$self->{queue}}, \%obj);
	} else {
		push(@{$self->{queue}}, \%obj);
	}
}

sub query_sync($) {
	my ($self, $query, $consistencylevel, $cb) = @_;

	my %obj = ();
	$obj{query} = $query;
	$obj{consistencylevel} = $consistencylevel;
	$obj{cb} = $cb;

	my $cv = $self->_query(\%obj);
	$cv->recv;	
}

sub _query($) {
	my ($self, $queryobj) = @_;

	my %obj = %{$queryobj};
	my $query = $obj{query};
	my $consistencylevel = $consistency_levels{$obj{consistencylevel}};
	

	if(!defined($consistencylevel)) {
		$consistencylevel = $consistency_levels{ONE};
	}

	my $cv = AE::cv {};

	my $handle = $self->{handle};

	my $body = encode_long_string($query);

	$cv->begin();

	#grab an available stream
	my $streamobj = shift(@{$self->{avaliablestreams}});
	
	if(!defined($streamobj)) {
		print "connection has max pending async requests. queueing request\n";
	}

	$streamobj->{queryobj} = \%obj;

	$self->{inusestreams}[$streamobj->{id}] = $streamobj;
	$handle->push_write(cassandra_bin => $version_codes{REQUEST},
		$flags{NONE}, $streamobj->{id}, $request_codes{QUERY}, $body.$consistencylevel);

	$handle->push_read(cassandra_bin => sub {
		my ($msg) = @_;

		#get this stream object now that we have a result from C*
		my $streamobj = $self->{inusestreams}[$msg->{stream}];
		my %qobj = %{$streamobj->{queryobj}};

		if($msg->{opcode} eq $response_codes{RESULT}) {
			my $result = decode_result($msg->{body});
			$qobj{result} = $result;

			if(defined($qobj{cb})) {
				$qobj{cb}(\%qobj);
			}
		} elsif ($msg->{opcode} eq $response_codes{ERROR}) {
			warn "Encountered an error: $msg->{body}\n";
			
		} else {
			warn "Unknown result from C*. Please file a bug.\n";
		}

		# release stream object and cleanup so other blocked tasks can use it
		$streamobj->{queryobj} = undef;
		$self->{avaliablestreams}[$msg->{stream}] = $streamobj;
		$self->{inusestreams}[$msg->{stream}] = undef;


		$cv->end();
	});
	return $cv;
}

sub prepare() {
	my ($self, $query) = @_;

	my $cv = AE::cv {};

	my $handle = $self->{handle};

	my $body = encode_long_string($query);

	$cv->begin();
	$handle->push_write(cassandra_bin => $version_codes{REQUEST},
		$flags{NONE}, 0, $request_codes{PREPARE}, $body);

	$handle->push_read(cassandra_bin => sub {
		my ($msg) = @_;

		if($msg->{opcode} eq $response_codes{RESULT}) {
			my $result = decode_result($msg->{body});	
		} elsif ($msg->{opcode} eq $response_codes{ERROR}) {
			warn "Encountered an error: $msg->{body}\n";
		} else {
			warn "Unknown result from C*. Please file a bug.\n";
		}

		$cv->end();
	});
	return $cv;
}

sub execute() {
	my ($self, $id, $values, $consistencylevel) = @_;

	#<id><n><value_1>....<value_n><consistency>
	if(!defined($consistencylevel)) {
		$consistencylevel = $consistency_levels{ONE};
	}

	my $cv = AE::cv {};

	my $handle = $self->{handle};

	my $body = encode_short($id).encode_short(scalar(@{$values}));

	foreach my $val (@{$values}) {
		$body .= encode_bytes($val);	
	}

	$body .= $consistencylevel;

	$cv->begin();
	$handle->push_write(cassandra_bin => $version_codes{REQUEST},
		$flags{NONE}, 0, $request_codes{EXECUTE}, $body);

	$handle->push_read(cassandra_bin => sub {
		my ($msg) = @_;

		if($msg->{opcode} eq $response_codes{RESULT}) {
			my $result = decode_result($msg->{body});
		} elsif ($msg->{opcode} eq $response_codes{ERROR}) {
			warn "Encountered an error: $msg->{body}\n";
		} else {
			warn "Unknown result from C*. Please file a bug.\n";
		}

		$cv->end();
	});
	return $cv;
}

1;
