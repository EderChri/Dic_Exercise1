import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class MrCatCount(MRJob):
    '''
    Class that defines the map reduce job used to count the number of reviews per category
    '''

    def steps(self):
        '''
        This method defines the steps of this map reduce job. First all reviews are mapped to <category, 1>. Then
        the reducer sums the instances per category up and returns the category and the sum.
        :return: A list of MRStep objects with mapper and reducer set
        '''
        return [
            MRStep(mapper=self.map_cat,
                   reducer=self.sum_cat)
        ]

    def map_cat(self, _, line):
        '''
        Mapper: Maps a review to <key, value>-pair
        :param _: Key from the raw input
        :type _: None
        :param line: one review as a raw input line. Any trailing newline characters are stripped by MRJob
        :type line: str
        :return: Returns a key value pair of category as key and 1 as value, both two JSONs separated by a tab
        '''
        l = json.loads(line)
        cat = l['category']
        yield cat, 1

    def sum_cat(self, cat, values):
        '''
        Reducer: Sums up the number of reviews per category
        :param cat: Category as Key
        :type cat: str
        :param values: Contains all 1s for the category
        :type values: generator (collection of values)
        :return: Returns a key value pair of category as key and the number of reviews per category as value, both JSONs
        separated by a tab
        '''
        yield cat, sum(values)

if __name__ == '__main__':
    MrCatCount.run()