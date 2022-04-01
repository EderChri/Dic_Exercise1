import sys
import json
import re
from mrjob.job import MRJob
import collections
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol

class MrFin(MRJob):

    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer0)
        ]

    def mapper(self, _, line):
        words_dict = dict(json.loads(line.split('\t')[1]))
        yield _, list(words_dict.keys())

    def reducer0(self, _, values):
        result = []
        for value in list(values):
            result += value
        yield None, str((sorted(set(result))))[1:-1]


if __name__ == '__main__':
    MrFin.run()
