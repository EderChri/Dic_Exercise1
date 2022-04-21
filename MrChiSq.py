import json
import re
import ast
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol


class MrChiSq(MRJob):
    '''
    Class that defines the map reduce job that gets words, occurrences of the words per category and total number of
    occurrences of the word as input and returns
    the 75 most distinguishing words (valued in chi square value) per category.
    '''

    # Properties that are set in the mapper_init
    total_counter = 0
    cat_counter = {}

    # Defines the local files needed
    FILES = ['CatCount.txt']

    # Defines the output protocol to avoid quotes around strings in output.
    OUTPUT_PROTOCOL = RawProtocol


    def steps(self):
        '''
        This method defines the steps of this map reduce job. The first step calculates the chi square value and gets
        the 75 most distinguishing words. The second step sorts the categories and polishes the output
        :return: A list of MRStep objects with mapper, combiner and reducer set
        '''
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.get_chisq_value,
                   reducer=self.group_per_cat),
            MRStep(reducer=self.sort_cats)
        ]

    def mapper_init(self):
        '''
        This method is run once before mapper and initializes the total review counter and the counter per category
        by reading it in.
        :return: None
        '''
        with open("CatCount.txt") as f:
            self.total_counter = int(f.readline())
            self.cat_counter = json.loads(f.readline().replace("'", '"'))

    def get_chisq_value(self, _, line):
        '''
        Mapper: Calculates the chi square value for each word and category
        :param _: Key from the raw input
        :type _: None
        :param line: A line containing a (word, category) pair and a (count per category, total number of occurrences)
        pair
        :type line: str
        :return: a key value pair, the category as key and a list containing the chi square value and the word as value
        '''

        line = str(line)
        # Replaces ' with " if line has wrong format
        if "'" in line:
            line = line.replace("'", '"')

        # Splits according to the first tab or first space, depending on the selected protocol
        if "\t" in line:
            key, value = line.split('\t', 1)
        else:
            key, value = re.split(r"\]\s\[", line)
            key += "]"
            value = "[" + value
        # Reads in the string as list
        [word, cat] = ast.literal_eval(key)
        [A, n_word] = ast.literal_eval(value)

        B = n_word - A
        C = self.cat_counter[cat] - A
        D = self.total_counter - A - B -C
        chi_sq = self.total_counter * (A * D - B * C) ** 2 / (
                (A + B) * (A + C) * (B + D) * (C + D))
        yield cat, [chi_sq, word]

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
        Reducer: Sorts the categories alphabetically and adds the final line
        :param _: Key
        :type _: None
        :param values: All categories and their
        :type values: generator (collection of lists)
        :return: key value pair as raw string already optimized to fit the required output format. Key is the category
        with <> and values are the words as well as their chi square values.
        '''
        words = set()
        for cat, result in sorted(values):
            # Adds <> around category and cuts of the {} of the dictionary as well as replaces all unnecessary
            # whitespaces, quotes and apostrophes. Multiple replace() calls are not beautiful but efficient
            # See https://stackoverflow.com/questions/3411771/best-way-to-replace-multiple-characters-in-a-string
            words.update(result.keys())
            yield None ,'<' + str(cat) +'>' + " " +str(result)[1:-1].replace('"','').replace(' ','').replace(',',' ').replace("'", '')
        # Write last line containing all the words to output
        # Sorts the words first alphabetically, casts list to str and removes brackets
        # Then removes all whitespaces, and '. At the end replaces all commas with whitespaces to achieve desired format
        yield None, str(sorted(list(words)))[1:-1].replace(' ', '').replace("'",'').replace(',', ' ')

if __name__ == '__main__':
    MrChiSq.run()
