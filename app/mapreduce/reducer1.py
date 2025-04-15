#!/usr/bin/env python3
import sys
from collections import defaultdict

def main():
    # Dictionary to store term -> {doc_id: tf} mappings
    term_doc_tf = defaultdict(dict)
    
    # Read input from stdin
    for line in sys.stdin:
        # Parse the input line
        # Expected format: term\tdoc_id\ttf
        parts = line.strip().split('\t')
        if len(parts) != 3:
            continue
        
        term = parts[0]
        doc_id = parts[1]
        tf = int(parts[2])
        
        # Store the term frequency for this document
        term_doc_tf[term][doc_id] = tf
    
    # Output: term\tdoc_id1:tf1,doc_id2:tf2,...
    for term, doc_tf in term_doc_tf.items():
        doc_tf_str = ','.join([f"{doc_id}:{tf}" for doc_id, tf in doc_tf.items()])
        print(f"{term}\t{doc_tf_str}")

if __name__ == "__main__":
    main()