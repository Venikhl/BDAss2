#!/usr/bin/python3

import sys
import re
from collections import Counter
import uuid

def tok_text(document):
    return re.findall(r'[a-zA-Z0-9]+', document.lower())

doc_id = str(uuid.uuid4())
total_terms = Counter()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    terms = tok_text(line)
    total_terms.update(terms)

for term, count in total_terms.items():
    print(f"{doc_id}\t{term}\t{count}")
