#!/bin/bash

SUITE=$1
SCALE=$2
STREAM=$3
OUTPUT=$4
QUERY=$5

rm -f $OUTPUT
for ((i=0; i<$STREAM; i++)); do
  echo "perl runTpSuite.pl $SUITE $SCALE $i $STREAM $QUERY >> $OUTPUT &"
  perl runTpSuite.pl $SUITE $SCALE $i $STREAM $QUERY >> $OUTPUT &
done
echo "waiting finished"
wait
echo "finished"
