from pyspark.sql import SparkSession


def generate_buckets(genre_iter):
    genre_buckets = []
    for genre_row in genre_iter:
        genre_col = genre_row['genre']
        if genre_col:
            genre_bucket = genre_col.split(', ')
            genre_buckets.append(genre_bucket)

    return genre_buckets


def to_genre_set(genre_iter):
    genre_buckets = generate_buckets(genre_iter)

    genres_set = set()
    for bucket in genre_buckets:
        genres_set = genres_set.union(set(bucket))

    return genres_set


sess = SparkSession.builder.appName('biganimedata').getOrCreate()
df = sess.read.csv(header=True, path='data/anime.csv').rdd.repartition(numPartitions=6)

genres = df.mapPartitions(f=to_genre_set).map(lambda x: {x}).reduce(set.union)

sorted_genre = sorted(list(genres))
print(sorted_genre)
print(len(sorted_genre))
