#!/bin/bash
echo "This script includes commands to run mapreduce jobs using hadoop streaming to index documents"

# Default input path
INPUT_PATH="/data"
if [ ! -z "$1" ]; then
    INPUT_PATH="$1"
fi

echo "Input path is: $INPUT_PATH"

# Check if the input path exists in HDFS
if [[ $INPUT_PATH == /* ]]; then
    echo "Checking if HDFS directory exists: $INPUT_PATH"
    if ! hdfs dfs -test -d $INPUT_PATH; then
        echo "Creating HDFS directory: $INPUT_PATH"
        hdfs dfs -mkdir -p $INPUT_PATH
        
        # If local data directory exists, copy its contents to HDFS
        if [ -d "data" ]; then
            echo "Copying local data directory to HDFS..."
            hdfs dfs -put -f data/* $INPUT_PATH/
        else
            echo "Warning: Local data directory not found. HDFS directory is empty."
        fi
    fi
fi

# Create temporary directory for intermediate results
echo "Creating temporary directories..."
hdfs dfs -rm -r -f /tmp/index
hdfs dfs -mkdir -p /tmp/index

# Step 1: Extract document ID, title, and content from input files
echo "Step 1: Extracting document information..."
if [[ $INPUT_PATH == /* ]]; then
    # HDFS path
    # Create a temporary file to store all document information
    TEMP_FILE="/tmp/doc_info.txt"
    touch $TEMP_FILE
    
    # List all files in the input directory
    echo "Listing files in HDFS directory: $INPUT_PATH"
    hdfs dfs -ls $INPUT_PATH
    
    # Process each file
    for FILE in $(hdfs dfs -ls $INPUT_PATH | grep -v "^d" | awk '{print $8}'); do
        echo "Processing file: $FILE"
        
        # Extract filename without path
        FILENAME=$(basename $FILE)
        
        # Extract document ID and title from filename
        # Assuming filename format: id_title.txt
        ID_TITLE=${FILENAME%.txt}
        
        # Get file content
        CONTENT=$(hdfs dfs -cat $FILE)
        
        # Append to temporary file
        echo -e "$ID_TITLE\t$CONTENT" >> $TEMP_FILE
    done
    
    # Check if we have any data
    if [ ! -s $TEMP_FILE ]; then
        echo "Error: No data was extracted from the input files"
        exit 1
    fi
    
    echo "Created temporary file with $(wc -l < $TEMP_FILE) lines"
    
    # Copy to HDFS
    hdfs dfs -put -f $TEMP_FILE /tmp/doc_info.txt
    rm $TEMP_FILE
else
    # Local path
    # Create a temporary file to store all document information
    TEMP_FILE="/tmp/doc_info.txt"
    touch $TEMP_FILE
    
    echo "Listing files in local directory: $INPUT_PATH"
    ls -la $INPUT_PATH
    
    # Process each file in the input directory
    for FILE in $INPUT_PATH/*; do
        # Skip if not a regular file
        if [ ! -f "$FILE" ]; then
            echo "Skipping non-file: $FILE"
            continue
        fi
        
        echo "Processing file: $FILE"
        
        # Extract filename without path
        FILENAME=$(basename $FILE)
        
        # Extract document ID and title from filename
        # Assuming filename format: id_title.txt
        ID_TITLE=${FILENAME%.txt}
        
        # Get file content
        CONTENT=$(cat $FILE)
        
        # Append to temporary file
        echo -e "$ID_TITLE\t$CONTENT" >> $TEMP_FILE
    done
    
    # Check if we have any data
    if [ ! -s $TEMP_FILE ]; then
        echo "Error: No data was extracted from the input files"
        exit 1
    fi
    
    echo "Created temporary file with $(wc -l < $TEMP_FILE) lines"
    
    # Copy to HDFS
    hdfs dfs -put -f $TEMP_FILE /tmp/doc_info.txt
    rm $TEMP_FILE
fi

# Verify the input file exists in HDFS
echo "Verifying input file in HDFS:"
hdfs dfs -ls /tmp/doc_info.txt
echo "Input file content (first 5 lines):"
hdfs dfs -cat /tmp/doc_info.txt | head -n 5

# Step 2: Run MapReduce job to create document index
echo "Step 2: Creating document index..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.framework.name=local \
    -input /tmp/doc_info.txt \
    -output /tmp/index/document_index \
    -mapper "python3 mapreduce/mapper1.py" \
    -reducer "python3 mapreduce/reducer1.py" \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py

# Step 3: Run MapReduce job to calculate document statistics
echo "Step 3: Calculating document statistics..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.framework.name=local \
    -input /tmp/doc_info.txt \
    -output /tmp/index/document_stats \
    -mapper "python3 mapreduce/mapper2.py" \
    -reducer "python3 mapreduce/reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py

# Step 4: Run MapReduce job to calculate term statistics
echo "Step 4: Calculating term statistics..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.framework.name=local \
    -input /tmp/doc_info.txt \
    -output /tmp/index/vocabulary \
    -mapper "python3 mapreduce/mapper3.py" \
    -reducer "python3 mapreduce/reducer3.py" \
    -file mapreduce/mapper3.py \
    -file mapreduce/reducer3.py

# Step 5: Copy results from HDFS to local filesystem
echo "Step 5: Copying results from HDFS..."
mkdir -p /tmp/index
hdfs dfs -getmerge /tmp/index/document_index/part-* /tmp/index/document_index.txt
hdfs dfs -getmerge /tmp/index/document_stats/part-* /tmp/index/document_stats.txt
hdfs dfs -getmerge /tmp/index/vocabulary/part-* /tmp/index/vocabulary.txt

# Verify the output files
echo "Verifying output files:"
ls -la /tmp/index/
echo "Document index content (first 5 lines):"
head -n 5 /tmp/index/document_index.txt
echo "Document stats content (first 5 lines):"
head -n 5 /tmp/index/document_stats.txt
echo "Vocabulary content (first 5 lines):"
head -n 5 /tmp/index/vocabulary.txt

# Clean up temporary files
echo "Cleaning up temporary files..."
hdfs dfs -rm -r -f /tmp/doc_info.txt
hdfs dfs -rm -r -f /tmp/index

# Step 6: Store index data in Cassandra
echo "Step 6: Storing index data in Cassandra..."
python3 /app/mapreduce/cassandra_store.py

echo "Indexing completed successfully!"
