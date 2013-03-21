#!/usr/bin/perl

## 2013-03-21 Thu
# This file relates to github issue #28.
# https://github.com/mkjellman/perlcassa/issues/28
#
# CQL3 exec(): parameter hash doesn't work for int, uuid and timeuuid type columns
#
##


use perlcassa;
use UUID::Tiny;
use Try::Tiny;

use Data::Dumper;


my $cassy = new perlcassa(
    'keyspace' => "testspace",
    'do_not_discover_peers' => 1,
    'hosts' => ['localhost'],
    'write_consistency_level' => Cassandra::ConsistencyLevel::QUORUM,
    'read_consistency_level' => Cassandra::ConsistencyLevel::QUORUM,
    'port' => '9160'
);

try {
    $cassy->exec(
        "CREATE TABLE testtable (key uuid," .
        " ivalue int," .
        " tvalue text," .
        " PRIMARY KEY(key))"
    );
} catch {
    print "Probably table existed before\n";
};


my $key = create_UUID_as_string();
try {
    $cassy->exec(
        "INSERT INTO testtable (key, ivalue, tvalue)" .
        " VALUES ($key, 123, 'teststring1')"
    );
    print "Test 1 succeeded\n";
} catch {
    print "Test 1 failed: ".Dumper(\$_)."\n";
};

$key = create_UUID_as_string();
try {
    $cassy->exec(
        "INSERT INTO testtable (key, ivalue, tvalue)" .
        " VALUES (:keyvalue, 234, 'teststring2')",
        {
            keyvalue => $key
        }
    );
    print "Test 2 succeeded\n";
} catch {
    print "Test 2 failed: ".Dumper(\$_)."\n";
};

$key = create_UUID_as_string();
try {
    $cassy->exec(
        "INSERT INTO testtable (key, ivalue, tvalue)" .
        " VALUES ($key, :ivalue, 'teststring3')",
        {
            ivalue => 345,
        }
    );
    print "Test 3 succeeded\n";
} catch {
    print "Test 3 failed: ".Dumper(\$_)."\n";
};

$key = create_UUID_as_string();
try {
    $cassy->exec(
        "INSERT INTO testtable (key, ivalue, tvalue)" .
        " VALUES ($key, 456, :tvalue)",
        {
            tvalue => 'teststring4'
        }
    );
    print "Test 4 succeeded\n";
} catch {
    print "Test 4 failed: ".Dumper(\$_)."\n";
};
