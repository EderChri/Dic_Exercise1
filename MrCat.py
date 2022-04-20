import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

class MrCat(MRJob):
    '''
    Class that defines the map reduce job that gets reviews as input and returns the number of occurrences per word
    per category
    '''

    # Defines the local files needed
    FILES = ['stopwords.txt']

    def steps(self):
        '''
        This method defines the steps of this map reduce job. First all valid words (no stopwords,...) in each review
        are mapped to <word, category>. The combiner counts the number of occurrences per word for each category.
        The reducer sums up the output of the combiner and emits <word, dictionary> where dictionary contains the
        occurrences of the word for each category.
        :return: A list of MRStep objects with mapper, combiner and reducer set
        '''
        return [
            MRStep(mapper=self.word_cat_pairs_per_review,
                   combiner=self.word_per_cat,
                   reducer=self.reduce_words_per_cat)
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

        # Counts the mapper calls => equal to number of reviews after job
        self.increment_counter('mapper calls', 'counter')
        # Counts the mapper calls for each category => equal to number of reviews per category after job
        self.increment_counter('catcounter', cat)
        # This line splits per special character, digit, tab or whitespace
        content_str = re.split(f"\s|\t|[0-9]|[{re.escape(special_string)}]", content_str)
        # Removes duplicates
        content_set = set(content_str)

        # For every word that is not a stop word yield the category and the word as key value pair.
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield word, cat

    def word_per_cat(self, word, cats):
        '''
        Combiner: Counts the occurrences of word for each category
        :param word: Word as key
        :type word: str
        :param cats: List of categories where the word occurs
        :type cats: list of strings
        :return: Returns a key value pair of word (key) and a dictionary of number of word occurrences (value)
        '''

        yield word, dict(Counter(cats))

    def reduce_words_per_cat(self, word, word_count_dicts):
        '''
        Reducer: Sums up the dictionaries received from the combiner.
        :param word: Word as key
        :type word: str
        :param word_count_dicts: A list of dictionaries of occurrences of Word per category
        :type word_count_dicts: generator (list of dictionaries with string as key and int as value)
        :return: Returns a key value pair of word (key) and a dictionary of number of word occurrences (value)
        '''

        word_count = Counter({})

        # Merges multiple dictionaries into one according to key
        for word_dic in word_count_dicts:
            new_count = Counter(word_dic)
            word_count.update(new_count)
        yield word, dict(word_count)

if __name__ == '__main__':
    MrCat.run()
