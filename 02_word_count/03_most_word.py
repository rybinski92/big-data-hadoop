from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRMostWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_words,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_keys,
                   reducer=self.reducer_max)
        ]

    def mapper_words(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word.lower(), 1


    def reducer(self, word, counts):
        yield word, sum(counts)

    def mapper_keys(self, key, value):
        yield None, (value, key)

    def reducer_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MRMostWord.run()