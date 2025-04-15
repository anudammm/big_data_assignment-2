#!/usr/bin/env python3
import sys
from collections import defaultdict

def main():
    # Dictionary to store term -> document count mappings
    term_doc_count = defaultdict(set)
    
    # Read input from stdin
    for line in sys.stdin:
        # Parse the input line
        # Expected format: term\tdoc_id
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue
        
        term = parts[0]
        doc_id = parts[1]
        
        # Add document ID to the set for this term
        term_doc_count[term].add(doc_id)
    
    # Output: term\tdoc_count
    for term, doc_ids in term_doc_count.items():
        doc_count = len(doc_ids)
        print(f"{term}\t{doc_count}")

if __name__ == "__main__":
    main() 