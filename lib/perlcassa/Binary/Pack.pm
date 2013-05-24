package perlcassa::Binary::Pack;

use strict;
use warnings;
use utf8;

use base 'Exporter';
our @EXPORT = qw(encode_int encode_short
		encode_string encode_long_string
		encode_uuid encode_string_list
		encode_option encode_option_list
		encode_string_map encode_string_multimap
		
		decode_result
		);

use perlcassa::Util;
use perlcassa::Decoder;

use UUID::Tiny;

our %result_types = (
	'VOID'		=> '01',
	'ROWS'		=> '02',
	'SET_KEYSPACE'	=> '03',
	'PREPARED'	=> '04',
	'SCHEMA_CHANGE'	=> '05'
);

our %rows_flags = (
	'GLOBAL_TABLES_SPEC'	=> '01'
);

our %option_ids = (
	"0" 	=> 'custom',
	"1"	=> 'ascii',
	"2"	=> 'bigint',
	"3"	=> 'blob',
	"4"	=> 'boolean',
	"5"	=> 'counter',
	"6"	=> 'decimal',
	"7"	=> 'double',
	"8"	=> 'float',
	"9"	=> 'int',
	"10"	=> 'text',
	"11"	=> 'timestamp',
	"12"	=> 'uuid',
	"13"	=> 'varchar',
	"14"	=> 'varint',
	"15"	=> 'timeuuid',
	"16"	=> 'inet',
	"32"	=> 'list',
	"33"	=> 'map',
	"34"	=> 'set'
);

# [int] A 4 bytes integer
sub encode_int($) {
	my $int = shift;

	return pack('l>',$int);
}

# [short] A 2 bytes unsigned integer
sub encode_short($) {
	my $short = shift;

	# max 2^16-1
	unless (($short > 65535) || ($short =~ /^[d]*/)) {
		warn "[$short] cannot be incoded, it must be a max of 2^16-1\n";
		return pack('c',0).pack('c',0);
	}

	return pack('n',$short); 
}

# [string] A [short] n, followed by n bytes representing a UTF-8 string
sub encode_string($) {
	my $string = shift;

	my $packed_len = encode_short(length($string));
	return pack('a* a*', $packed_len, $string);
}

# [long string] A [int] n, followed by n bytes representing a UTF-8 string
sub encode_long_string($) {
	my $string = shift;

	my $packed_len = encode_int(length($string));
	return pack('a* a*', $packed_len, $string);
}

# [uuid] A 16 bytes long uuid
sub encode_uuid() {
	return create_UUID_as_string();
}

# A [short] n, followed by n [string]
sub encode_string_list($) {
	my $strings = shift;

	my $tmp_str_list;
	foreach my $string (@{$strings}) {
		$tmp_str_list .= encode_string($string);
	}

	my $num_elm = encode_short(scalar(@{$strings}));
	return pack('a* a*', $num_elm, $tmp_str_list);
}

# A [int] n, followed by n bytes if n >= 0
# if n < 0, no byte should follow and the value represented is 'null'
sub encode_bytes($) {
	my $bytes = shift;

	my $len = length($bytes);

	my $str;
	if($len >= 0) {
		$str = encode_int($len).pack('a*',$bytes);
	} else {
		#null
		$str = encode_int(-1);
	}

	return $str;
}

sub decode_bytes($) {
	my $bytes = shift;

	my $len = decode_int(substr($bytes, 0, 4));
	
	if(defined($len) && $len > 0) {
		return substr($bytes, 4, $len);
	}

	return undef;
}

# [short bytes] A [short] n, followed by n bytes if n >= 0
sub encode_short_bytes($) {
	my $bytes = shift;

	if ($bytes < 0) {
		warn "[short bytes] cannot be negative\n";
		return encode_short(0);
	}

	my $len = encode_short(length($bytes));
	return $len.pack('a*',$bytes);
}

sub decode_short_bytes($) {
	my $bytes = shift;

	my $len = decode_short(substr($bytes, 0, 2));
	my $bytesdec = unpack("a$len", $bytes);
	return $bytesdec;
}

# [option] A pair of <id><value> where <id> is a [short] representing
# the options id and <value> depends on that options (and can be
# of size 0). THe supported id (and the corresponding <value>) 
# will be described when this is used
sub encode_option($$) {
	my ($id, $value) = @_;

	return encode_short($id).pack('a*',$value);
}

sub decode_option($) {
	my $option = shift;

	my $optid = decode_short(substr($option,0,2));

	my $val = '';
	unless (length($option) == 2) {
		$val = decode_string(substr($option,2,length($option)));
	}

	return $optid, $val;
}

# [option list] A [short] n, followed by n [option]
sub encode_option_list($) {
	my $options = shift;

	my $str;

	foreach my $option (@{$options}) {
		$str .= $option;
	}

	return encode_short(scalar(@{$options})).$str;
}

# [inet] An address (ip and port) to a node. It consists of one [byte] n,
# that represents the address size, followed by n [byte] representing the
# IP address (in practice n can only be either 4 (IPv4) or 16(IPv6), following
# by one [int] representing the port
sub encode_inet($) {
	my ($ip, $port) = @_;

	my $address_size;
	my $ip_address;
	my $ip_type;

	my $str;

	if(defined(isValidIPv4($ip))) {
		my $addr = inet_aton($ip);
		$address_size = encode_byte(length($addr));
		$ip_address = pack('a*', $addr);
		$ip_type = encode_byte(4);
	} elsif (defined(isValidIPv6($ip))) {
		my $addr = inet_pton($ip);
		$address_size = encode_byte(length($addr));
		$ip_address = pack('a*', $addr);
		$ip_type = encode_byte(6);
	} else {
		warn "Invalid IP Address specified [$ip]\n";
	}

	$str = $address_size.$ip_address.$ip_type.encode_int($port);
	
	return $str;
}

# A [short] n, followed by n pair <k><v> where <k> and <v> are [string]
sub encode_string_map($) {
	my $strings = shift;
	
	my $str;

	foreach my $ref (@{$strings}) {
		my %map = %{$ref};
		foreach my $key (keys %map) {
			my $map_elm = encode_string($key).encode_string($map{$key});
			$str .= $map_elm;
		}
	}

	my $elm_num = encode_short(scalar(@{$strings}));

	return $elm_num.$str;
}

# [string multimap] A [short] n, followed by n pair <k><v> where <k> is a
# [string] and <v> is a [string list]
# takes a array of hashes, where the value is an array of UTF-8 bytes
sub encode_string_multimap($) {
	my $maps = shift;

	my $str;

	foreach my $ref (@{$maps}) {
		my %map = %{$ref};
		foreach my $key (keys %map) {
			$str .= encode_string_list($key);
		}	
	}

	my $elm_num = encode_short(scalar(@{$maps}));

	return $elm_num.$str;
}


sub decode_int($) {
	my $int = shift;

	return unpack('l>', $int);
}

sub decode_short($) {
	my $short = shift;

	return unpack('n1', $short);
}

sub decode_string($) {
	my $string = shift;

	my $len = decode_short(substr($string, 0, 2));
	my $str = unpack("a*",substr($string, 2, $len));
	return $str; 
}

sub decode_long_string($) {
	my $string = shift;

	my $len = decode_int(substr($string, 0, 4));
	my $str = unpack("a*",substr($string, 2, $len));
	return $str; 

}

sub decode_result($) {
	my $body = shift;

	my $kind = decode_int(substr($body, 0, 4));

	if($kind == $result_types{VOID}) {
		#result had no information, sucessful
		return 1;
	} elsif ($kind == $result_types{ROWS}) {
		#results for a select query, now deserialize our set of rows
		my $decoded = decode_rows(substr($body, 4, length($body)));
		return $decoded;
	} elsif ($kind == $result_types{SET_KEYSPACE}) {
		#result to a 'use' query
	} elsif ($kind == $result_types{PREPARED}) {
		# result to a PREPARE message
	} elsif ($kind == $result_types{SCHEMA_CHANGE}) {
		# result to a schema altering query
	} else {
		warn "Unknown Result Type. Please file a bug\n";
		return undef;
	}
	
}

# see 4.2.5.2 of the native protocol spec
sub decode_rows($) {
	my $rows = shift;
	my $offset = 0;

	my $flags = decode_int(substr($rows, $offset, 4));
	$offset = $offset + 4;

	# the number of columns selected by the query this result is of.
	# defined the number of <col_spec_i> elements in and the number
	# of elements in and the number of elements for each row in the <rows_content>
	my $columns_count = decode_int(substr($rows, $offset, 4));
	$offset = $offset + 4;

	my $global_tables_spec_total_len = 0;
	if ($flags == $rows_flags{GLOBAL_TABLES_SPEC}) {
		my $keyspace_name_len = decode_short(substr($rows, $offset, 2));
		my $keyspace_name = decode_string(substr($rows, $offset, $keyspace_name_len+2));
		my $table_name_len = decode_short(substr($rows, $offset+$keyspace_name_len+2, 2));
		my $table_name = decode_string(substr($rows, $offset+$keyspace_name_len+2, $table_name_len+2));
	
		$global_tables_spec_total_len = $keyspace_name_len + $table_name_len + 4;
	}
	$offset = $offset + $global_tables_spec_total_len; 

	#<col_spec_i>
	my @columnsdefs;
	my @columnnames;
	for(my $i = 0; $i < $columns_count; $i++) {
		my $colname_len = decode_short(substr($rows, $offset, 2));
		my $colname = decode_string(substr($rows, $offset, $colname_len+$offset+2));
		my ($optid, $optval) = decode_option(substr($rows, $offset+$colname_len+2, 2));
		push(@columnsdefs, $optid);
		push(@columnnames, $colname);
		$offset = $offset + $colname_len + 2 + 2;
	}

	#<rows_count>
	my $rows_count = decode_int(substr($rows, $offset, 4));
	$offset = $offset + 4;

	my $rows_content;
	my %row = ();
	my $content_consumed = 0;
	if($rows_count > 0) {
		#<rows_content>
		$rows_content = substr($rows, $offset, length($rows));
		$row{result_count} = $rows_count;
		for (my $r = 0; $r < $rows_count; $r++) {
			my $rowname;
			my %colres = ();
			for (my $c = 0; $c < $columns_count; $c++) {
				# what is our def for this column?
				my $coltype = $option_ids{$columnsdefs[$c]};
				my $colname = $columnnames[$c];
				my $len = decode_int(substr($rows_content, $content_consumed, 4));

				my $colval;
				#is the column null for this key?
				if($len == -1) {
					$content_consumed = $content_consumed + 4;
				} else {
					$colval = decode_bytes(substr($rows_content, $content_consumed, $len+4));
					$colval = unpack_val($colval, $coltype);
					$content_consumed = $content_consumed + $len + 4;	

					#first column is the row
					if($c == 0) {
						$rowname = $colval;	
					}
				}

				$colres{$colname} = $colval;
			}
			push(@{$row{$rowname}},\%colres); 
		}
	}

	return \%row;
}

# see 4.2.5.4 of the native protocol spec
sub decode_prepared($) {
	my $prepared = shift;
	
	my $uuid = decode_short_bytes(substr($prepared, 0, 20));
	my $metadata = decode_row(substr($prepared, 2, length($prepared)));	
}

1;
