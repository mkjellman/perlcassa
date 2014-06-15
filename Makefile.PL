# Cassandra::perlcassa client for Apache Cassandra
#
# Copyright 2013 Michael Kjellman
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use 5.008_008;
use ExtUtils::MakeMaker;

my $live_tests = 0;

my %config = (
	NAME		=> 'perlcassa',
	VERSION_FROM	=> 'lib/perlcassa.pm',
	LICENSE		=> 'apache',
	PREREQ_PM	=> {
		'Thrift::XS' => '0',
		'UUID::Tiny' => '0',
		'Class::Accessor' => '0',
		'Time::HiRes'	=> '0',
		'Socket6'	=> '0'
	},
    dist        => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean       => { FILES => "Debian_CPANTS.txt Perlcassa-*.tar.gz" },
);

my @patterns = qw( t/*.t );
$config{test} = { TESTS => join ' ', map { glob } @patterns };

print "=  => To run full tests requires a running local cassandra server <=  =\n" unless $live_tests;

if ($live_tests) {
    # TODO Allow selection of keyspace
    # TODO Allow selection of test server?
    # Adds the tests from the 99live* folders
    my @livepatterns = qw( t/99live*/*.t );
    my $testfiles = join ' ', map { glob } @livepatterns ;
    $config{test}->{TESTS} .= " ".$testfiles;
}
else {
    print "Setting to skip live tests.\n";
}

WriteMakefile( %config );
