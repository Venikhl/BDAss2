#!/usr/bin/python3
import sys
import re
from collections import defaultdict
from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['172.20.0.2'], connect_timeout=10)
session = cluster.connect('indexer')
terms = defaultdict(lambda: defaultdict(int))
terms_count = defaultdict(int)

for l in sys.stdin:
    l = l.strip()
    if not l:
        continue
    did, t, c = l.split("\t")
    c = int(c)
    did = uuid.UUID(did)

    terms[did][t] += c
    terms_count[t] += 1



for did, terms in terms.items():
    for term, term_count in terms.items():
        session.execute(
            """
            INSERT INTO document_index (doc_id, term, term_count)
            VALUES (%s, %s, %s)
            """, (did, term, term_count)
        )


for term, d_count in terms_count.items():
    row = session.execute(
        "SELECT doc_frequency, total_documents FROM bm25_statistics WHERE term = %s",
        (term,)
    ).one()

    if row:
        frequency = row.doc_frequency + d_count
    else:
        frequency = d_count

    session.execute(
        """
        INSERT INTO bm25_statistics (term, doc_frequency, total_documents)
        VALUES (%s, %s, %s)
        """, (term, frequency, len(terms))
    )
