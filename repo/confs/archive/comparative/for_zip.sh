#!/bin/bash
date=$(date +%Y-%m-%d)
host="root@bdpe833n3"
dir="/home/BigData_SQL_Comparative"

for i in hot homr hos hos_gen sparksql;
do
  zip ${i}.zip $i
done

ssh $host mkdir ${dir}/config/${date}
scp *.zip $host:${dir}/config/${date}
rm -rf *.zip
