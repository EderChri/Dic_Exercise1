import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class MrCatCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  reducer=self.reducer)
        ]

    def mapper(self, _, line):
        l = json.loads(line)
        cat = l['category']
        yield cat, 1

    def reducer(self, cat, values):
        yield cat, sum(values)

if __name__ == '__main__':
    MrCatCount.run()