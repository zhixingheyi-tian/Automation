#!/usr/bin/perl

use warnings;
use POSIX qw(strftime);
use sort 'stable';

# User settings:
use constant DELIMITER  => ",";				# Changes this to generate tsv csv..etc
use constant PRINT_TIME => 0;				# Print / not print time stamp: 1 / 0 

#Structure of one line in log file:
#EX: exalytics2	      10	 1433905683	    cpu0	      %usr	        63.42
#    MACHINE_NAME, INTERVAL, TIME_INDEX, CPU_INDEX, COUNTER_INDEX, DATA_VALUE_INDEX
# 		  0			  1		     2           3             4              5
use constant {
	TIME_INDEX 		 => 2,
	CPU_INDEX 		 =>	3,
	COUNTER_INDEX	 => 4,
	DATA_VALUE_INDEX => 5
};

#----------------------------#
# Main function starts here. #
#----------------------------#
if ( @ARGV != 2 ) {
  die ("Usage: sar2csv.pl <input> <output>\n");
}

$sarFileName = "$ARGV[0]";
$outputFileName = "$ARGV[1]";
$endTime = 0;

$dataArray = loadLogFile($sarFileName);		# Loads the file content to dataArray.
@dataArray = sort byTime @$dataArray;		# Sorts the dataArray by time.
writeToFile ($outputFileName, \@dataArray);	# Writes the sorted dataArray to output file.

#--------------------------#
# Functions Library below. #
#--------------------------#

# Time comparison function for sorting.
sub byTime {
	@dataLineA = split(" ", $a);
	@dataLineB = split(" ", $b);

	$dataLineA[TIME_INDEX] <=> $dataLineB[TIME_INDEX];
}

# Accepts epoch time $epochTime as input. Returns H:M:S format time stamp.
sub epochToTimeStamp {
	$epochTime = $_[0];

	return strftime("%H:%M:%S",localtime($epochTime));
}

# Returns the initial epoch time from the input, which is a sorted dataArray.
# Since dataArray is sorted, just retrive the first epoch time from first
# element in dataArray. 
sub getInitialTime {
	$dataArray = $_[0];

	@initialDataLine = split(" ", @$dataArray[0]);
	return $initialDataLine[TIME_INDEX];	
}

# Takes a sar file's name as input, and returns its content as an array,
# where one element corresponds to one line in the file.
sub loadLogFile {
	$fileName = $_[0];

	open(FILE, "<$fileName") || die "Unable to open $sarFileName!\n";
	@dataArray = <FILE>;
	close(FILE);
	
	@lastLine = split(" ", $dataArray[-1]);
	$endTime = $lastLine[TIME_INDEX];
	return \@dataArray;
}

# Writes content of array $dataArray to output file $fileName. 
sub writeToFile {
	$fileName = $_[0];
	$dataArray = $_[1];

	writeHeader($fileName, $dataArray);
	writeData($fileName, $dataArray);
}

# Writes header of array $dataArray to output file $fileName.
sub writeHeader {
	$fileName = $_[0];
	$dataArray = $_[1];

	open(FILE, ">$fileName") || die "Unable to open $fileName!\n";

	if (PRINT_TIME) {
		print FILE "epoch" . DELIMITER;
		print FILE "time stamp" . DELIMITER;
	}

	$initTime = getInitialTime($dataArray);
	$lineNum = 0;
	for (;;) {
		@dataLine = split(" ", @$dataArray[$lineNum]);
		$currTime = $dataLine[TIME_INDEX];

		if ($currTime ne $initTime) {
			last;
		}
		if ($dataLine[CPU_INDEX] eq "-") {
			$header = $dataLine[COUNTER_INDEX];
		} else {
			$header = $dataLine[CPU_INDEX]."_".$dataLine[COUNTER_INDEX];
		}
		print FILE "$header" . DELIMITER;
		$lineNum++;
	}
	print FILE "\n";
	close(FILE);
}

# Writes content of array $dataArray to output file $fileName.
sub writeData {
	$fileName = $_[0];
	$dataArray = $_[1];

	open(FILE, ">>$fileName") || die "Unable to open $fileName!\n";

	$oldTime = getInitialTime($dataArray);		
	if (PRINT_TIME) {
		$timeStamp = epochToTimeStamp($oldTime);
		print FILE "$oldTime" . DELIMITER;
		print FILE "$timeStamp" . DELIMITER;
	}

	foreach $line (@$dataArray) {				
		@dataLine = split(" ", $line);
		$currTime = $dataLine[TIME_INDEX];
		
		# Skips the last line for the following buggy behavior:
		# The number of samples for each counter might differ by 1, thus in the last line some 
		# counters are missing, causing this script to fill in the wrong value
		
		if ($currTime eq $endTime) {
			last;
		}
		$dataValue = $dataLine[DATA_VALUE_INDEX];

		if ($currTime ne $oldTime) {
			$oldTime = $currTime;
			print FILE "\n";
			if (PRINT_TIME) {
				$timeStamp = epochToTimeStamp($currTime);
				print FILE "$currTime" . DELIMITER;
				print FILE "$timeStamp" . DELIMITER;
			}			
		}
		
		# If no value exists, put on "no value".
		if (not defined $dataValue) {
			$dataValue = "no value";
		}
		
		print FILE "$dataValue" . DELIMITER;
	}

	
	close(FILE);
}




