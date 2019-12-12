for i in {1987..2008}; do
  echo "Start Download for Year:  " $i
  wget http://stat-computing.org/dataexpo/2009/$i.csv.bz2
  echo "Complete Download for Year: " $i
  echo "Start Unzip for Year: " $i
  bzip2 -dk $i.csv.bz2
  echo "Complete Unzip for Year: " $i
  echo "Remove Header for Year:  " $i
  sed -i "1 d" "$i.csv"
  echo "Done All for Year:  " $i
done
echo "Merging all files"
cat *csv > final.csv
#awk 'FNR > 1' *.csv > consolidated.csv

