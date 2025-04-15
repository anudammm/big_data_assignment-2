#!/usr/bin/env python3
import sys
import re
from pyspark import SparkContext
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# BM25 parameters
k1 = 1.5  # Term frequency saturation parameter
b = 0.75  # Document length normalization parameter

def clean_text(text):
    # Convert to lowercase and remove special characters
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return text

def tokenize(text):
    # Split text into words
    return text.split()

def get_document_stats(session):
    # Get document statistics from Cassandra
    print("Retrieving document statistics from Cassandra...")
    rows = session.execute("SELECT * FROM search_index.document_stats")
    doc_stats = {}
    count = 0
    for row in rows:
        doc_stats[row.doc_id] = {
            'title': row.title,
            'term_count': row.term_count,
            'avg_doc_length': row.avg_doc_length,
            'total_docs': row.total_docs
        }
        count += 1
    print(f"Retrieved {count} document statistics")
    return doc_stats

def get_term_stats(session, terms):
    # Get term statistics from Cassandra
    print(f"Retrieving term statistics for {len(terms)} terms...")
    term_stats = {}
    for term in terms:
        rows = session.execute("SELECT * FROM search_index.vocabulary WHERE term = %s", (term,))
        for row in rows:
            term_stats[term] = row.doc_count
            print(f"Term '{term}' appears in {row.doc_count} documents")
    print(f"Retrieved statistics for {len(term_stats)} terms")
    return term_stats

def get_document_index(session, terms):
    # Get document index from Cassandra
    print(f"Retrieving document index for {len(terms)} terms...")
    doc_index = {}
    for term in terms:
        rows = session.execute("SELECT * FROM search_index.document_index WHERE term = %s", (term,))
        term_docs = {}
        for row in rows:
            term_docs[row.doc_id] = row.tf
        if term_docs:
            doc_index[term] = term_docs
            print(f"Term '{term}' appears in {len(term_docs)} documents")
    print(f"Retrieved index for {len(doc_index)} terms")
    return doc_index

def calculate_bm25_score(term, doc_id, tf, doc_stats, term_stats, total_docs):
    # Calculate IDF (Inverse Document Frequency)
    doc_count = term_stats.get(term, 1)  # Default to 1 to avoid division by zero
    idf = max(0, (total_docs - doc_count + 0.5) / (doc_count + 0.5))
    
    # Calculate document length normalization
    doc_length = doc_stats[doc_id]['term_count']
    avg_doc_length = doc_stats[doc_id]['avg_doc_length']
    length_normalization = (1 - b + b * doc_length / avg_doc_length)
    
    # Calculate term frequency normalization
    tf_normalization = (tf * (k1 + 1)) / (tf + k1 * length_normalization)
    
    # Calculate BM25 score
    return idf * tf_normalization

def main():
    # Get query from command line arguments
    if len(sys.argv) < 2:
        print("Usage: query.py <query>")
        sys.exit(1)
    
    query = ' '.join(sys.argv[1:])
    print(f"Query: {query}")
    
    # Clean and tokenize the query
    cleaned_query = clean_text(query)
    query_terms = tokenize(cleaned_query)
    print(f"Query terms: {query_terms}")
    
    # Connect to Cassandra
    print("Connecting to Cassandra...")
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    print("Connected to Cassandra")
    
    # Get document statistics
    doc_stats = get_document_stats(session)
    if not doc_stats:
        print("Error: No document statistics found in Cassandra")
        cluster.shutdown()
        sys.exit(1)
    
    # Get term statistics
    term_stats = get_term_stats(session, query_terms)
    if not term_stats:
        print("Warning: No term statistics found for the query terms")
    
    # Get document index
    doc_index = get_document_index(session, query_terms)
    if not doc_index:
        print("Warning: No document index found for the query terms")
    
    # Calculate BM25 scores for each document
    print("Calculating BM25 scores...")
    doc_scores = {}
    for term in query_terms:
        if term in doc_index:
            for doc_id, tf in doc_index[term].items():
                if doc_id in doc_stats:
                    score = calculate_bm25_score(
                        term, 
                        doc_id, 
                        tf, 
                        doc_stats, 
                        term_stats, 
                        doc_stats[doc_id]['total_docs']
                    )
                    doc_scores[doc_id] = doc_scores.get(doc_id, 0) + score
    
    # Sort documents by score
    sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Print top 10 results
    print("\nTop 10 relevant documents:")
    print("--------------------------")
    if sorted_docs:
        for i, (doc_id, score) in enumerate(sorted_docs[:10]):
            title = doc_stats[doc_id]['title']
            print(f"{i+1}. Document ID: {doc_id}, Title: {title}, Score: {score:.4f}")
    else:
        print("No relevant documents found for the query.")
    
    # Close connection
    cluster.shutdown()

if __name__ == "__main__":
    main()