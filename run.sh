DATA_SET_PATH=hdfs:///user/pknees/amazon-reviews/full/reviewscombined.json

python3 MapRedCatCount.py -r hadoop $DATA_SET_PATH > CatCount.txt
python3 MapRedChiSq.py -r hadoop $DATA_SET_PATH > output.txt
python3 MapRedSummary.py output.txt >> CatCount.txt
