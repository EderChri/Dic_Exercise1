import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol

class MrCat(MRJob):
    '''
    Class that defines the map reduce job that gets reviews as input and outputs the sorted categories with their
    75 most distinguishable words and their chi squared value.
    '''

    # Defines the local files needed
    FILES = ['stopwords.txt', 'CatCount.txt']
    # Sets the output protocol so strings instead of json format is printed
    OUTPUT_PROTOCOL = TextProtocol

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
                   reducer_init=self.init_get_cat_count,
                   reducer=self.get_A_C),
            MRStep(reducer_init=self.init_get_cat_count,
                   reducer=self.get_chi_sq),
            MRStep(reducer=self.group_per_cat),
            MRStep(reducer=self.sort_cats)
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
        special_string = '()[]{}.!?,;:+=-_"`~#@&*%€$§/\\1234567890\t'+"'"
        l = json.loads(line)
        [cat, content] = [l['category'], l['reviewText']]
        content_str = content.lower()
        # The following line creates a mapping table that maps every special character to an empty string
        # Then every special character is replaced with translate. Next any leading or trailing whitespaces are removed
        # In the last step the content string is split by any arbitrary number of whitespaces, as these can occur due to
        # dots being replaced by whitespaces.
        content_str = re.split(r'\s+',
            str.strip(
                content_str.translate(
                    str.maketrans(special_string, ' '*len(special_string)))
                )
            )
        # Removes duplicates
        content_set = set(content_str)

        # For every word that is not a stop word yield the category and the word as key value pair.
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield cat, word

    def init_get_cat_count(self):
        '''
        Initial Reducer: This method is called before any reducer in the step is run. It is used to read in the number
        of reviews and the number of reviews per category which were written by another map reduce job
        (see MapRedCatCount.py) earlier. Both counts are stored in variables of the object.
        :return: None
        '''
        self.cat_count = {}
        self.N = 0

        # This for loop gets the counts per category saves them in a variable and gets the total number of reviews
        for l in open('CatCount.txt'):
            [cat, count] = l.split("\t")
            cat = str(cat).replace('"', '')
            self.cat_count[cat] = int(count)
            self.N += int(count)

    def get_A_C(self, cat, words):
        '''
        Reducer: This calculates A (number of reviews in category which contain word) and C
        (number of reviews in categor ynot containing word) for the chi-square value for each word in category.
        :param cat: Category as key
        :type cat: str
        :param words: Contains all words from reviews of given category cat.
        :type words: generator (collection of words)
        :return: key value pair of word (str) and a list of category (str), the A (float) and C (float) values of the
        word with the given category cat.
        '''
        A = {}

        # Cast generator to list
        word_list = list(words)

        # For each word count the number of occurrences
        for word in word_list:
            if word in A.keys():
                A[word] += 1
            else:
                A[word] = 1

        for word in word_list:
            # For each word get C by taking the number of reviews of the given category and subtract A
            C = self.cat_count[cat]-A[word]
            yield word, [cat, A[word], C]

    def get_chi_sq(self, word, values):
        '''
        Reducer: Calculates the chi squared value
        :param word: word as key
        :type cat: str
        :param values: Contains lists of categories, A and C value for each word-category-combination
        :type words: generator (collection of lists)
        :return: key value pair of category (key) and a list of chi square value and the word
        '''

        # Cast generator to list
        value_list = list(values)
        word_count = {}

        # Count the number of occurrences of the word in each category
        for content in value_list:
            cat = content[0]
            if cat in word_count.keys():
                word_count[cat] += 1
            else:
                word_count[cat] = 1
        cat_set = set()
        for content in value_list:
            [cat, A, C] = content
            # Check if cat word combination already was yielded
            if cat in cat_set:
                continue
            # Add to set so this won't be calculated again
            cat_set.add(cat)
            # Get B by taking the number of times the word occurs in the category and subtract A
            B = sum(word_count.values())-A
            # Get D by taking the total number of reviews and subtracting A, B and C
            D = self.N - (A+B+C)
            # Get chi square value
            chi_sq_val = self.N*(A*D-B*C)**2/((A+B)*(A+C)*(B+D)*(C+D))
            yield cat, [chi_sq_val, word]

    def group_per_cat(self, cat, values):
        '''
        Reducer: Gets the 75 most distinguishable words per category
        :param cat: Category as key
        :type cat: str
        :param values: Contains lists of chi squared values and words for the given category
        :type values: generator (collection of lists)
        :return: key value pair with None as key (as it is not needed) and the 75 most distinguishable per category,
        their chi square value as dictionary and the category as value.
        '''
        result = {}
        for value, word in sorted(values, reverse=True):
            if len(result) == 75:
                break
            result[word] = value
        yield None, (cat, result)

    def sort_cats(self, _, values):
        '''
        Reducer: Sorts the categories alphabetically
        :param _: Key
        :type _: None
        :param values: All categories and their
        :type values: generator (collection of lists)
        :return: key value pair as raw string already optimized to fit the required output format. Key is the category
        with <> and values are the words as well as their chi square values.
        '''
        for cat, result in sorted(values):
            # Adds <> around category and cuts of the {} of the dictionary as well as replaces all unnecessary
            # whitespaces, quotes and apostrophs. Multiple replace() calls are not beautiful but efficient
            # See https://stackoverflow.com/questions/3411771/best-way-to-replace-multiple-characters-in-a-string
            yield '<' + str(cat) +'>', str(result)[1:-1].replace('"','').replace(' ','').replace(',',' ').replace("'", '')

if __name__ == '__main__':
    MrCat.run()