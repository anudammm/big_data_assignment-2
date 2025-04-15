#!/usr/bin/env python3
import sys
import os
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def wait_for_cassandra(cluster, max_attempts=10, delay=5):
    """Wait for Cassandra to become available"""
    for attempt in range(max_attempts):
        try:
            session = cluster.connect()
            print("Successfully connected to Cassandra")
            return session
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Failed to connect to Cassandra (attempt {attempt + 1}/{max_attempts}). Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise Exception("Failed to connect to Cassandra after maximum attempts") from e

def create_keyspace_and_tables(session):
    try:
        # Create keyspace if it doesn't exist
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_index
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        
        # Use the keyspace
        session.execute("USE search_index")
        
        # Create vocabulary table
        session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            doc_count int
        )
        """)
        
        # Create document index table
        session.execute("""
        CREATE TABLE IF NOT EXISTS document_index (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
        """)
        
        # Create document statistics table
        session.execute("""
        CREATE TABLE IF NOT EXISTS document_stats (
            doc_id text PRIMARY KEY,
            title text,
            term_count int,
            avg_doc_length float,
            total_docs int
        )
        """)
        print("Successfully created keyspace and tables")
    except Exception as e:
        print(f"Error creating keyspace and tables: {str(e)}")
        raise

def store_vocabulary(session, vocabulary_file):
    if not os.path.exists(vocabulary_file):
        print(f"Warning: Vocabulary file {vocabulary_file} does not exist")
        return
    
    try:
        # Prepare statement for vocabulary table
        stmt = session.prepare("INSERT INTO vocabulary (term, doc_count) VALUES (?, ?)")
        
        # Read vocabulary file and insert into Cassandra
        with open(vocabulary_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) != 2:
                    continue
                
                term = parts[0]
                doc_count = int(parts[1])
                
                session.execute(stmt, (term, doc_count))
        print("Successfully stored vocabulary data")
    except Exception as e:
        print(f"Error storing vocabulary data: {str(e)}")
        raise

def store_document_index(session, index_file):
    if not os.path.exists(index_file):
        print(f"Warning: Document index file {index_file} does not exist")
        return
    
    try:
        # Prepare statement for document index table
        stmt = session.prepare("INSERT INTO document_index (term, doc_id, tf) VALUES (?, ?, ?)")
        
        # Read index file and insert into Cassandra
        with open(index_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) != 2:
                    continue
                
                term = parts[0]
                doc_tf_pairs = parts[1].split(',')
                
                for doc_tf in doc_tf_pairs:
                    doc_id, tf = doc_tf.split(':')
                    tf = int(tf)
                    
                    session.execute(stmt, (term, doc_id, tf))
        print("Successfully stored document index data")
    except Exception as e:
        print(f"Error storing document index data: {str(e)}")
        raise

def store_document_stats(session, stats_file):
    if not os.path.exists(stats_file):
        print(f"Warning: Document stats file {stats_file} does not exist")
        return
    
    try:
        # Prepare statement for document statistics table
        stmt = session.prepare("INSERT INTO document_stats (doc_id, title, term_count, avg_doc_length, total_docs) VALUES (?, ?, ?, ?, ?)")
        
        # Read stats file and insert into Cassandra
        with open(stats_file, 'r') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) != 5:
                    continue
                
                doc_id = parts[0]
                title = parts[1]
                term_count = int(parts[2])
                avg_doc_length = float(parts[3])
                total_docs = int(parts[4])
                
                session.execute(stmt, (doc_id, title, term_count, avg_doc_length, total_docs))
        print("Successfully stored document statistics data")
    except Exception as e:
        print(f"Error storing document statistics data: {str(e)}")
        raise

def main():
    try:
        # Connect to Cassandra
        cluster = Cluster(['cassandra-server'])
        session = wait_for_cassandra(cluster)
        
        # Create keyspace and tables
        create_keyspace_and_tables(session)
        
        # Store vocabulary
        store_vocabulary(session, '/tmp/index/vocabulary.txt')
        
        # Store document index
        store_document_index(session, '/tmp/index/document_index.txt')
        
        # Store document statistics
        store_document_stats(session, '/tmp/index/document_stats.txt')
        
        # Close connection
        cluster.shutdown()
        print("Successfully completed all operations")
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 