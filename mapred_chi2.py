import sys
import json
import re
from mrjob.job import MRJob

class MrCat(MRJob):

    FILES = ['stopwords.txt']

    # A ... number of reviews with word w in category c
    # B ... number of reviews with word w not in category c
    # C ... number of reviews in c without w
    # D ... number of reviews without w not in c

    # _, ["Electronic", "word", id, 1]
    # A = sum(1 where cat = c and word = w) über id
    # B = sum(1 where cat != c and word = w) über id
    # C = sum(1 where cat = c and word != w) über id
    # D = sum(1 where cat != c and word != c) über id

    # <cat> , [word:value, word:value]

    # _, [cat, id, word, 1]
    # Catcount = sum(id where cat = c)
    # N = sum(id)
    # word, [cat, 1, catcount, N]
    #

    def mapper(self, _, line):
        stops = set(i.strip() for i in open('stopwords.txt'))

        # special_string = list('()[]{}.!?,;:+=-_"`~#@&*%€$§\/1234567890'+"'")
        # replacement_string = list([None for i in special_string])
        # replace_dict = dict(zip(special_string, replacement_string))
        l = json.loads(line)
        [cat, content, uid] = [l['category'], l['reviewText'], l['reviewerID'] + l['asin'] + l['unixReviewTime']]
        content_str = content.lower()
        content_set = set(re.sub("[^A-Za-z\s]+", ' ', content_str).split(' '))
        for word in content_set.difference(stops):
            if len(word) > 0:
                yield _, [cat, word, uid, 1]

    def reducer(self, key, values):
        value_list = list(values)
        transpose = list(map(list, zip(*value_list)))
        cat_list, word_list, uid_list = transpose[0], transpose[1], transpose[2]

    def reducer_old2(self, key, values):
        cat_counter = {}
        value_list = list(values)
        for content in value_list:
            [word, cnt] = [content[0], content[1]]
            if word not in cat_counter.keys():
                cat_counter[word] = cnt
            else:
                cat_counter[word] += cnt
        for content in value_list:
            [cat, word] = [key, content[0]]
            yield cat, [word, cat_counter[word]]

    def reducer_org(self, key, values):

        def init_cat_map(cat_map):
            for word in cat_map:
                cat_map[word] = 0.0
            return cat_map

        def readin(fields):
            [word, cat, cnt] = fields
            if word not in cat_map:
                cat_map[word] = 1.0
            cat_map[word] += float(cnt)

        def output(cat_map):
            if len(cat_map) == 0:
                return
            for word in cat_map:
                cnt = cat_map[word]
                yield key, [word, cnt]

        current_word = None
        cat_map = {}
        for content in list(values):
            [cat, word, cnt] = [key, content[0], content[1]]
            if word != current_word:
                output(cat_map)
                current_word = word
                cat_map = init_cat_map(cat_map)

        readin([cat, word, cnt])
        current_cat = cat




if __name__ == '__main__':
    MrCat.run()
    # if sys.argv[1] == 'm':
    #     mapper()
    # if sys.argv[1] == 'r':
    #     reducer()
    # if sys.argv[1] == 'r1':
    #     cat_reducer()
    # if sys.argv[1] == 'm2':
    #     cat_mapper()


    def reducer_old():
        def init_word_map(word_map):
            for cat in A_C:
                word_map[cat] = 0.0
            return word_map

        def readin(fields):
            [word, cat, cnt] = fields
            if cat not in word_map:
                word_map[cat] = 1.0
            word_map[cat] += float(cnt)

        def output(word_map):
            if len(word_map) == 0:
                return
            A_B = sum([word_map[cat] for cat in word_map])
            C_D = N - A_B

            for cat in word_map:
                A = word_map[cat]
                B = A_B - A
                C = A_C[cat] - A
                D = C_D - C
                chi_squar = N*(A*D-B*C)**2 / A_C[cat] / A_B / B_D[cat] / C_D
                print('\t'.join(map(str, [cat, current_word, chi_squar, A, B, C, D,
                                          1 if A*D > B*C else -1])))

        A_C = {}
        B_D = {}
        N = 0.0
        ratio_file = sys.argv[2]
        with open(ratio_file, 'r') as f:
            for line in f:
                [cat, cnt] = line.strip('\n').split('\t')
                A_C[cat] = float(cnt)
                N += float(cnt)
        for cat in A_C:
            B_D[cat] = N - A_C[cat]

        current_word = None
        word_map = {}
        for line in sys.stdin:
            [word, src, cnt] = line.strip('\n').split('\t')
            if word != current_word:
                output(word_map)
                current_word = word
                word_map = init_word_map(word_map)

            readin([word, src, cnt])
            current_word = word

        output(word_map)