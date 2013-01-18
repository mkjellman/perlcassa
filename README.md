# NAME

perlcassa - Perl Client for Apache Cassandra

# VERSION

v0.041

## INSTALLATION
perlcassa can be installed by doing the usual:
```bash
    $ perl Makefile.PL
    $ make
    $ make install
```

# SYNOPSIS

```perl
    use perlcassa;

    my $obj = new perlcassa(
        'columnfamily' => 'myCF',
        'keyspace' => 'myKeyspace',
        'hosts' => ['host1.cassandra.local',
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
    print "Row key, col01: ".$row->{key}->{value}.", ".$row->{col01}->{value}."\n";

```


# REQUIRES

Perl5.10, Thrift::XS, Time::HiRes 

# DESCRIPTION

perlcassa is a native Perl client for interfacing with Apache Cassandra. It is essentially an API for Apache Thrift. It intelligently deals with CompositeType columns and ValidationClasses and encodes and packs them appropriately for the columnfamily specified. perlcassa deals with connection pooling, automatic retrying of insertions, automatic serialization and deserialization of primitive data types to pass column validation classes and more.

Although other Perl Cassandra clients exist such as Cassandra::Lite and Net::Cassandra they have not been updated for many of the changes in Cassandra releases >0.80. They also do not serialize and deserialize data making them not much more than an abstraction of the base Thrift calls. In my experence the difficulty lies in validation classes and being fault tolerant, not abstracting the Thrift code.

The module name perlcassa follows the naming convention of other Cassandra clients such as phpcassa and pycassa. This module is included on CPAN for convinence however, please see https://github.com/mkjellman/perlcassa for active development.

Note: This package does not support SuperColumns. Please look into CompositeType Comparators instead.

# DOCUMENTATION

Documentation is light at this time and will be improved in the next version. For now please refer to the documentation available in POD format or look through the examples in the examples folder.

# METHODS

## Creation

- new perlcassa()

    Creates a new Apache Cassandra Perl Client

# TODO

- better documentation
- better handling thrift exceptions to try from another provided Cassandra instance/host automagically
- general performance optimizations
- auto retry failures where the node is up when the client is created but there is an exception such as a timeout on insert
- better CQL3 support

# ACKNOWLEDGEMENTS

This wouldn't have been possible without help from my friend and colleague BJ Black.

# AUTHOR

Michael Kjellman, mkjellman@barracuda.com

# COPYRIGHT & LICENSE

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
