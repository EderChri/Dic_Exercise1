import sys
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

class MrFin(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer)
        ]

    def mapper(self, _, line):
        words_dict = dict(json.loads(line.split('\t')[1]))
        yield _, list(words_dict.keys())

    def reducer(self, _, values):
        result = []
        for value in list(values):
            result += value
        yield None, str((sorted(set(result))))[1:-1]


if __name__ == '__main__':
    MrFin.run()
