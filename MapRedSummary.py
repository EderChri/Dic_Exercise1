import sys
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import itertools

class MrFin(MRJob):
    '''
    Class that defines the map reduce job used to add the final line with the most used words to the output
    '''

    # Following line defines the output protocol so there are no empty key lines before the values (words) in the output
    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        '''
        This method defines the steps of this map reduce job. First all words regardless of their categories are
        mapped together. Then the reducer sorts them in prints them in the correct format.
        :return: A list of MRStep objects with mapper and reducer set
        '''
        return [
            MRStep(mapper=self.get_words_per_cat,
                   reducer=self.sort_words)
        ]

    def get_words_per_cat(self, _, line):
        '''
        Mapper: Maps a line of <category dictionary of words> to a <None, list of words> key value-pair
        :param _: Key from the raw input
        :type _: None
        :param line: One line with category and the 75 most distinguishing for the category
        :type line: str
        :return: Returns a key value pair of _ as key and the words of the category in a list
        '''
        # Takes a line of the format <category> word:value word:value... and splits it by spaces and taking all but the
        # first element therefore having all word:value word:value elements in a list. Then splitting each element of
        # said list and takes only the words of each element.
        words_list = [s.split(':')[0] for s in line.split(' ')[1:] ]
        yield _, words_list

    def sort_words(self, _, values):
        '''
        Reducer: Sorts all the words and returns them alphabetically sorted
        :param _: Key that is used to combine all words
        :type _: None
        :param values: collection of words
        :type values: generator (collection of lists of words)
        :return: Returns a key value pair of None (as key is not used here) and the alphabetically
        sorted words of all categories
        '''
        # This line is used to flatten the list of lists into a single list
        result = list(itertools.chain.from_iterable(list(values)))
        # The set is used to make the list of words unique, afterwards the words are sorted alphabetically.
        # Next it is cast to a string, the brackets are removed and last but not least the commas and quotes are removed
        # Double replace is not beautiful but efficient,
        # see https://stackoverflow.com/questions/3411771/best-way-to-replace-multiple-characters-in-a-string
        yield None, str((sorted(set(result))))[1:-1].replace(',','').replace("'", '')


if __name__ == '__main__':
    MrFin.run()
