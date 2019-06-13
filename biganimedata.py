from pyspark.sql import SparkSession
from collections import defaultdict
from itertools import combinations
from operator import add

MIN_IN_SET = 2
MAX_IN_SET = 5
SUPPORT_THRESHOLD = 100
PARTITIONS = 10
PARTITION_SUPPORT_THRESHOLD = int((1.0 / PARTITIONS) * SUPPORT_THRESHOLD)


def generate_buckets(genre_iter):
    genre_buckets = []
    for genre_row in genre_iter:
        genre_col = genre_row['genre']
        if genre_col:  # This ignores anime buckets that don't have a genre. Is there a way to inject here?
            genre_bucket = genre_col.split(', ')
            genre_buckets.append(genre_bucket)

    return genre_buckets


def to_genre_set(genre_iter):
    genre_buckets = generate_buckets(genre_iter)

    genres_set = set()
    for bucket in genre_buckets:
        genres_set = genres_set.union(set(bucket))

    return genres_set


def get_frequent_itemset(k, buckets, last_frequent_items=None):
    if k > MAX_IN_SET:
        return []

    plausible_items = None
    if last_frequent_items is not None:
        plausible_items = set()
        for itemset in last_frequent_items:
            itemset_unfolded = set(list(itemset))
            plausible_items = plausible_items.union(itemset_unfolded)

    non_frequent_count = defaultdict(int)
    for bucket in buckets:
        bucket_set = set(bucket)
        if plausible_items is not None:
            bucket_set.intersection_update(plausible_items)

        k_itemsets = combinations(bucket_set, k)

        for itemset in k_itemsets:
            sorted_tuple = tuple(sorted(list(itemset)))
            non_frequent_count[sorted_tuple] = min(non_frequent_count[sorted_tuple] + 1, PARTITION_SUPPORT_THRESHOLD)

    frequent_itemset = [itemset for itemset, count in non_frequent_count.items() if count == PARTITION_SUPPORT_THRESHOLD]

    return frequent_itemset + get_frequent_itemset(k+1, buckets, frequent_itemset)


def pass_it_son(genre_iter):
    return get_frequent_itemset(1, generate_buckets(genre_iter))


def pass_it_again_son(genre_iter, candidate_frequent_items):
    genre_buckets = generate_buckets(genre_iter)

    candidate_list = list(candidate_frequent_items)
    candidates_as_sets = [set(candidate_frequent_item) for candidate_frequent_item in candidate_list]
    item_count = defaultdict(int)
    for bucket in genre_buckets:
        bucket_set = set(bucket)
        for idx, candidate_set in enumerate(candidates_as_sets):
            if not candidate_set.difference(bucket_set):
                item_count[candidate_list[idx]] += 1

    return [(itemset, count) for itemset, count in item_count.items()]


sess = SparkSession.builder.appName('biganimedata').getOrCreate()
df = sess.read.csv(header=True, path='data/anime.csv')
print('Number of items: {}'.format(df.count()))

df = df.rdd.repartition(numPartitions=PARTITIONS)

genres = df.mapPartitions(f=to_genre_set).map(lambda x: {x}).reduce(set.union)

sorted_genre = sorted(list(genres))
print(sorted_genre)

candidate_frequent_items = df.mapPartitions(f=pass_it_son).map(lambda x: {x}).reduce(set.union)
frequent_items = df.mapPartitions(lambda partition: pass_it_again_son(partition, candidate_frequent_items))\
                .reduceByKey(add)\
                .filter(lambda keyvalue: len(keyvalue[0]) >= MIN_IN_SET and keyvalue[1] >= SUPPORT_THRESHOLD)\
                .sortBy(keyfunc=lambda keyvalue: keyvalue[1], ascending=False)\
                .map(lambda keyvalue: keyvalue[0])\
                .collect()

print(frequent_items)
