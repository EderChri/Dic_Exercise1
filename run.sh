
# Set path to data set
DATA_SET_PATH=hdfs:///user/pknees/amazon-reviews/full/reviewscombined.json

# Puts the stopwords.txt on the cluster if needed
# hadoop fs -put stopwords.txt

# Runs the script to run the first job and saves the output into tmp.txt
python3 runMrCat.py -r hadoop $DATA_SET_PATH > tmp.txt
# Runs the script to run the second job, taking the tmp.txt as input and saves all to output.txt
python3 MrChiSq.py -r hadoop tmp.txt > output.txt

