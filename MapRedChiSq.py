import sys
import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class MrCat(MRJob):

    FILES = ['stopwords.txt', 'CatCount.txt']

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.init_get_cat_count,
                   reducer=self.get_A_C),
            MRStep(reducer_init=self.init_get_cat_count,
                   reducer=self.get_chi_sq),
            MRStep(reducer=self.group_per_cat)
        ]

    def mapper(self, _, line):

        stops = set(i.strip() for i in open('stopwords.txt'))
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
        # removes duplicates
        content_set = set(content_str)
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield cat, word
    def init_get_cat_count(self):
        self.cat_count = {}
        self.N = 0
        for l in open('CatCount.txt'):
            [cat, count] = l.split("\t")
            cat = str(cat).replace('"', '')
            self.cat_count[cat] = int(count)
            self.N += int(count)

    def get_A_C(self, cat, words):
        A = {}
        word_list = list(words)
        for word in word_list:
            if word in A.keys():
                A[word] += 1
            else:
                A[word] = 1
        for word in word_list:
            C = self.cat_count[cat]-A[word]
            yield word, [cat, A[word], C]

    def get_chi_sq(self, word, values):
        value_list = list(values)
        word_count = {}
        for content in value_list:
            cat = content[0]
            if cat in word_count.keys():
                word_count[cat] += 1
            else:
                word_count[cat] = 1
        for content in value_list:
            [cat, A, C] = content
            B = sum(word_count.values())-A
            D = self.N - (A+B+C)
            chi_sq_val = self.N*(A*D-B*C)**2/((A+B)*(A+C)*(B+D)*(C+D))
            yield cat, [chi_sq_val, word]

    def group_per_cat(self, key, values):
        result = {}
        for value, word in sorted(values, reverse=True):
            if len(result) == 75:
                break
            result[word] = value
        yield key, result


if __name__ == '__main__':
    MrCat.run()
