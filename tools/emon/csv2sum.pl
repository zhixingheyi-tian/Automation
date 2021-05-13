#!/usr/bin/perl

if ( @ARGV != 2 ) {
  die ("Usage: csv2sum.pl <input> <output>\n");
}

$csv_file_name = "$ARGV[0]";
$summary_file_name = "$ARGV[1]";

$spikeThreshold = 50.0;

my(@headers);
my(@samples);
my(@numSamples);
my(@spikes);

open(CSVFILE, $csv_file_name) || die "Unable to open $csv_file_name!!!\n";
$lineNum=1;
for (;;) {
  $line = <CSVFILE>;
  
  #if ($lineNum == 2){
  #  $line = <CSVFILE>;
  #  $lineNum++;
  #  print "The first row of data is ignored.\n";
  #}   
  last unless defined($line);
  $colIdx=0;
  while ($line ne ""){
    if ($line =~ /([^,]*),(.*)/) {
      $field=$1;
      $line=$2;
    }else{
      $field=$line;
      $line="";
    }
    if ($field =~/^"(.*)"$/) {
      $field = $1;
    }
    #print $field."\n";
    if ($lineNum==1) {
      $headers[$colIdx]=$field;
      $samples[$colIdx]=0;
      $numSamples[$colIdx]=0;
      #print $headers[$colIdx]."\n";
    }else{
        $samples[$colIdx]+=$field;
        $numSamples[$colIdx]++;         
    }
    $colIdx++;
  } #while 
  $lineNum++;
}
close(CSVFILE);

open(SUMFILE, ">$summary_file_name") || die "Unable to open $summary_file_name!!!\n";
for ($i=0; $i<@headers; $i++) {
  #print $headers[$i]."\n";
 
   $avg = $samples[$i]/$numSamples[$i];

  print SUMFILE $headers[$i]."=".$avg."\n";
}
close(SUMFILE);
