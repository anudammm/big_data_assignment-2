#!/usr/bin/env python3
import sys
import re
import os

def clean_text(text):
    # Convert to lowercase and remove special characters
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return text

def tokenize(text):
    # Split text into words
    return text.split()

def main():
    # Read input from stdin
    for line in sys.stdin:
        # Parse the input line
        # Expected format: id_title\tcontent
        parts = line.strip().split('\t')
        if len(parts) < 2:
            continue
        
        id_title = parts[0]
        doc_content = parts[1]
        
        # Split id_title into doc_id and doc_title
        id_title_parts = id_title.split('_', 1)
        if len(id_title_parts) < 2:
            doc_id = id_title
            doc_title = ""
        else:
            doc_id = id_title_parts[0]
            doc_title = id_title_parts[1]
        
        # Clean and tokenize the content
        cleaned_content = clean_text(doc_content)
        terms = tokenize(cleaned_content)
        
        # Count term occurrences
        term_freq = {}
        for term in terms:
            if term:
                term_freq[term] = term_freq.get(term, 0) + 1
        
        # Output: term\tdoc_id
        for term in term_freq:
            print(f"{term}\t{doc_id}")

if __name__ == "__main__":
    main() 