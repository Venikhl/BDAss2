#!/usr/bin/python3

import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

query = sys.argv[1].strip().lower().split()

spark = SparkSession.builder \
    .appName("CassandraBM25Query") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "172.20.0.2") \
    .getOrCreate()

sc = spark.sparkContext
query_terms = set(query)

doc_index = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="document_index", keyspace="indexer") \
    .load() \
    .filter(col("term").isin(query_terms))

bm25_stats = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="bm25_statistics", keyspace="indexer") \
    .load() \
    .filter(col("term").isin(query_terms))


joined = doc_index.join(bm25_stats, on="term")

k1 = 1.5
b = 0.75

def bm25(row):
    tf = row.term_count
    df = row.doc_frequency
    N = row.total_documents
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
    score = idf * tf * (k1 + 1) / (tf + k1)
    return (str(row.doc_id), score)

scores = joined.rdd.map(bm25) \
                   .reduceByKey(lambda a, b: a + b) \
                   .takeOrdered(10, key=lambda x: -x[1])

for doc_id, score in scores:
    print(f"{doc_id}\t{score}")

spark.stop()
