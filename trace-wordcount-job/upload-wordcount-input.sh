#!/bin/bash

HADOOP_HOME=${HADOOP_HOME:-/users/oscarz08/hadoop-3.4.1}
INPUT_DIR="/user/$(whoami)/wordcount-input"
LOCAL_FILE="wordcount_sample.txt"

echo "--- Generating Extended WordCount Test Data ---"

cat <<EOF > $LOCAL_FILE
The quick brown fox jumps over the lazy dog.
The quick brown fox is fast and clever.
Lazy dogs don't jump over quick foxes.
The fox and the dog became friends in the forest.
Every morning, the quick fox would race with the lazy dog.
Sometimes the dog won, but usually the fox was faster.
One day, a clever crow watched them from a tall tree.
She wondered who would win the next morning’s race.
In the end, they both sat under the sun, tired but happy.
EOF

echo "✅ Created extended test file: $LOCAL_FILE"

echo "🚮 Removing existing HDFS directory: $INPUT_DIR (if any)"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash $INPUT_DIR > /dev/null 2>&1

echo "📁 Creating HDFS input directory: $INPUT_DIR"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $INPUT_DIR

echo "⏫ Uploading $LOCAL_FILE to $INPUT_DIR on HDFS"
$HADOOP_HOME/bin/hdfs dfs -put -f $LOCAL_FILE $INPUT_DIR/

echo "📋 Verifying content:"
$HADOOP_HOME/bin/hdfs dfs -ls $INPUT_DIR
$HADOOP_HOME/bin/hdfs dfs -cat $INPUT_DIR/$LOCAL_FILE

echo "--- WordCount Test Data Upload Complete ---"
