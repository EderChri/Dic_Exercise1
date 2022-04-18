DATA_SET_PATH=hdfs:///user/pknees/amazon-reviews/full/reviewscombined.json
hadoop fs -put stopwords.txt

python3 runMrCat.py -r hadoop $DATA_SET_PATH > tmp.txt
python3 runMrChiSq.py -r hadoop tmp.txt > output.txt

