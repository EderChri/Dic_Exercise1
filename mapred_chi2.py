import sys
import json
import re
from mrjob.job import MRJob
from mrjob.step import MRStep

class MrCat(MRJob):

    FILES = ['stopwords.txt']

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer0),
            MRStep(reducer=self.reducer1),
            MRStep(reducer=self.reducer2),
            MRStep(reducer=self.reducer3)
        ]

    def mapper(self, _, line):
        stops = set(i.strip() for i in open('stopwords.txt'))

        # special_string = list('()[]{}.!?,;:+=-_"`~#@&*%€$§\/1234567890'+"'")
        # replacement_string = list([None for i in special_string])
        # replace_dict = dict(zip(special_string, replacement_string))
        l = json.loads(line)
        [cat, content, uid] = [l['category'], l['reviewText'], str(l['reviewerID']) + str(l['asin']) + str(l['unixReviewTime'])]
        content_str = content.lower()
        content_set = set(re.sub("[^A-Za-z\s]+", ' ', content_str).split(' '))
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield _, [cat, word, uid]

    def reducer0(self, _, values):

        uid_set = set([])
        value_list = list(values)
        for content in value_list:
            uid = content[2]
            uid_set.add(uid)
        N = len(uid_set)
        for content in value_list:
            [cat, word, uid] = content
            yield cat, [word, uid, N]

    def reducer1(self, key, values):
        A = {}
        uid_set = set([])
        value_list = list(values)
        for content in value_list:
            [word, uid, _] = content
            uid_set.add(uid)
            if word not in A.keys():
                A[word] = 1
            else:
                A[word] += 1
        for content in value_list:
            [cat, word, uid, N] = [key, content[0], content[1], content[2]]
            C = len(uid_set)-A[word]
            yield word, [cat, A[word], C, uid, N]

    def reducer2(self, key, values):
        word = key
        uid_set = set([])
        value_list = list(values)
        for content in value_list:
            uid = content[3]
            uid_set.add(uid)
        for content in value_list:
            [cat, A, C, _, N] = content
            B = len(uid_set)-A
            D = N - B
            chi_sq_val = (A*D-B*C)**2/(A+B)/(A+C)/(B+D)/(C+D)
            yield cat, [chi_sq_val, word]

    def reducer3(self, key, values):
        result = {}
        for value, word in sorted(values, reverse=True):
            if len(result) == 75:
                break
            result[word] = value
        yield key, result


if __name__ == '__main__':
    MrCat.run()
