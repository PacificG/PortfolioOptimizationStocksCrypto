#!/bin/bash

echo "date,open,high,low,close,volume, Adj Close,Name" > all_$1_5yr.csv
cd individual_$1_2021_5years
files=$(ls *.csv)
for file in $files
do
	tail -n +2 $file >> ../all_$1_5yr.csv
done