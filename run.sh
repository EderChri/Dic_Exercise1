python3 MapRedCatCount.py $1 > CatCount.txt
python3 MapRedChiSq.py $1 > output.txt
python3 MapRedSummary.py output.txt >> output.txt