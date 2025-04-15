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
        
        # Count the number of terms in the document
        term_count = len([t for t in terms if t])
        
        # Output: doc_id\ttitle\tterm_count
        print(f"{doc_id}\t{doc_title}\t{term_count}")

if __name__ == "__main__":
    main() 