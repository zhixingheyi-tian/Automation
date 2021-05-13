#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;

# PROTOTYPES
sub dieWithUsage(;$);

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

# MAIN
dieWithUsage("one or more parameters not defined") unless @ARGV >= 1;
my $suite = shift;
my $scale = shift || 2;
my $number = shift;
dieWithUsage("suite name required") unless $suite eq "tpcds" or $suite eq "tpch";

chdir $SCRIPT_PATH;
if( $suite eq 'tpcds' ) {
	chdir "sample-queries-tpcds";
} else {
	chdir 'sample-queries-tpch';
} # end if
#my @queries = glob '*.sql';
my @queries = ();
if ( $number ) {
    push @queries, split(',',$number);
} else {
    push @queries, glob '*.sql';
}
my $db = { 
	'tpcds' => "tpcds_bin_partitioned_parquet_$scale",
	'tpch' => "tpch_flat_parquet_$scale"
};

print "filename,status,time,rows\n";
for my $query ( @queries ) {
    if ( $query =~ /^\d+$/ ) {
        $query = "query$query.sql";
    }		
	my $logname = "$query.log";
    my $querySetting = "$query.setting";
    my $queryRuntimeSetting = "$query.runtime.setting";
    `cat testbench.settings > $queryRuntimeSetting`;
    if(-e $querySetting){
        `cat $querySetting >> $queryRuntimeSetting`;
    }
    my $cmd="echo 'use $db->{${suite}}; source $query;' | hive -i $queryRuntimeSetting --hiveconf spark.app.name=tpc-ds-$query 2>&1  | tee $query.log";
#	my $cmd="echo 'use $db->{${suite}}; source $query;' | hive -i testbench.settings --hiveconf spark.app.name=$query 2>&1  | tee $query.log";
#	my $cmd="cat $query.log";
	#print $cmd ; exit;

	my $hiveStart = time();

	my @hiveoutput=`$cmd`;
	die "${SCRIPT_NAME}:: ERROR:  hive command unexpectedly exited \$? = '$?', \$! = '$!'" if $?;

	my $hiveEnd = time();
	my $hiveTime = $hiveEnd - $hiveStart;
	my $flag = 0;
	foreach my $line ( @hiveoutput ) {
		if( $line =~ /Time taken:\s+([\d\.]+)\s+seconds,\s+Fetched:\s+(\d+)\s+row/ ) {
			print "$query,success,$hiveTime,$2\n";
			$flag = 1;
		} elsif(
			$line =~ /^FAILED: /
			# || /Task failed!/
			) {
			print "$query,failed,$hiveTime\n";
			$flag = 1;
		}  # end if
	} # end while
	if ($flag == 0) {
		print "$query,success,$hiveTime,0\n";
	}
} # end for


sub dieWithUsage(;$) {
	my $err = shift || '';
	if( $err ne '' ) {
		chomp $err;
		$err = "ERROR: $err\n\n";
	} # end if

	print STDERR <<USAGE;
${err}Usage:
	perl ${SCRIPT_NAME} [tpcds|tpch] [scale]

Description:
	This script runs the sample queries and outputs a CSV file of the time it took each query to run.  Also, all hive output is kept as a log file named 'queryXX.sql.log' for each query file of the form 'queryXX.sql'. Defaults to scale of 2.
USAGE
	exit 1;
}

