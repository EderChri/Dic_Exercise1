import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

class MrCat(MRJob):
    '''
    Class that defines the map reduce job that gets reviews as input and outputs the sorted categories with their
    75 most distinguishable words and their chi squared value.
    '''

    # Defines the local files needed
    FILES = ['stopwords.txt']

    def steps(self):
        '''
        This method defines the steps of this map reduce job. First all valid words (no stopwords,...) in each review
        are mapped to <category, word>. Then the initial reducer gets the number of reviews as well as the number
        of reviews per category. The next reducer calculates the A and the C value. In the next step again the inital
        reducer gets the number of reviews as well as the number of reviews per category. The next reducer calculates
        the chi squared value. In the next step the 75 most distinguishable words are selected per category. In the last
        step all categories are alphabetically sorted.
        :return: A list of MRStep objects with mapper and reducer set
        '''
        return [
            MRStep(mapper=self.word_cat_pairs_per_review,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def word_cat_pairs_per_review(self, _, line):
        '''
        Mapper: Maps a review to pairs of categories and words (for every valid word (no stopwords,...) per review a
        pair is created.
        :param _: Key from the raw input
        :type _: None
        :param line: one review as a raw input line. Any trailing newline characters are stripped by MRJob
        :type line: str
        :return: Returns a key value pair of category (key) and word (value) in JSON format
        '''

        # Reads in the stop words, with the set() we get only unique ones
        stops = set(i.strip() for i in open('stopwords.txt'))
        # This string defines the delimiters to split by
        special_string = '()[]{}.!?,;:+=-_"`~#@&*%€$§/\\1234567890\t' + "'"
        l = json.loads(line)
        cat, content = l['category'], l['reviewText']
        content_str = content.lower()
        self.increment_counter('mapper calls', 'counter')
        self.increment_counter('catcounter', cat)
        # The following line creates a mapping table that maps every special character to an empty string
        # Then every special character is replaced with translate. Next any leading or trailing whitespaces are removed
        # In the last step the content string is split by any arbitrary number of whitespaces, as these can occur due to
        # dots being replaced by whitespaces.
        content_str = re.split(r'\s+',
                               str.strip(
                                   content_str.translate(
                                       str.maketrans(special_string, ' ' * len(special_string)))
                                   )
                               )
        # Removes duplicates
        content_set = set(content_str)

        # For every word that is not a stop word yield the category and the word as key value pair.
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield word, cat

    def combiner(self, word, cats):

        yield word, dict(Counter(cats))

    def reducer(self, word, word_count_dicts):

        word_count = Counter({})
        for word_dic in word_count_dicts:
            new_count = Counter(word_dic)
            word_count.update(new_count)
        yield word, dict(word_count)

if __name__ == '__main__':
    MrCat.run()
