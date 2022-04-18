import json
import re
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol


class MrChiSq(MRJob):
    total_counter = 0
    cat_counter = {}
    FILES = ['CatCount.txt']
    OUTPUT_PROTOCOL = RawProtocol
    words = set()

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper2,
                   reducer=self.group_per_cat),
            MRStep(reducer=self.sort_cats)
        ]

    def mapper_init(self):
        with open("CatCount.txt") as f:
            self.total_counter = int(f.readline())
            self.cat_counter = json.loads(f.readline().replace("'", '"'))

    def mapper2(self, _, line):
        word, content = str(line).split('\t', 1)
        A = dict(json.loads(content))
        chi_sq = {}
        for cat in A.keys():
            C = self.cat_counter[cat] - A[cat]
            B = sum(A.values()) - A[cat]
            D = self.total_counter - A[cat] - B - C
            chi_sq[cat] = self.total_counter * (A[cat] * D - B * C) ** 2 / (
                    (A[cat] + B) * (A[cat] + C) * (B + D) * (C + D))
            yield cat, [chi_sq[cat], word]

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
            self.words.add(word)
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
            # whitespaces, quotes and apostrophes. Multiple replace() calls are not beautiful but efficient
            # See https://stackoverflow.com/questions/3411771/best-way-to-replace-multiple-characters-in-a-string
            yield '<' + str(cat) +'>', str(result)[1:-1].replace('"','').replace(' ','').replace(',',' ').replace("'", '')

if __name__ == '__main__':
    MrChiSq.run()
