#!/usr/bin/perl

my $suite = shift;
my $scale = shift || 2;
my $stream = shift || 3;
my $output = shift;
my $query = shift;

my $hiveStart = time();
`sh ./runThroughput.sh $suite $scale $stream $output $query`;
my $hiveEnd = time();
my $hiveTime = $hiveEnd - $hiveStart;
print "Throughput finished in $hiveTime seconds\n";
