#!/usr/bin/env python3
import sys
from collections import defaultdict

def main():
    # Dictionary to store document statistics
    doc_stats = {}
    total_docs = 0
    total_terms = 0
    
    # Read input from stdin
    for line in sys.stdin:
        # Parse the input line
        # Expected format: doc_id\ttitle\tterm_count
        parts = line.strip().split('\t')
        if len(parts) != 3:
            continue
        
        doc_id = parts[0]
        doc_title = parts[1]
        term_count = int(parts[2])
        
        # Store document statistics
        doc_stats[doc_id] = {
            'title': doc_title,
            'term_count': term_count
        }
        
        total_docs += 1
        total_terms += term_count
    
    # Calculate average document length
    avg_doc_length = total_terms / total_docs if total_docs > 0 else 0
    
    # Output: doc_id\ttitle\tterm_count\tavg_doc_length\ttotal_docs
    for doc_id, stats in doc_stats.items():
        print(f"{doc_id}\t{stats['title']}\t{stats['term_count']}\t{avg_doc_length}\t{total_docs}")

if __name__ == "__main__":
    main() 