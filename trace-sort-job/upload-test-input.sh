#!/bin/bash

# 设置默认 Hadoop 安装路径
HADOOP_HOME=${HADOOP_HOME:-/users/oscarz08/hadoop-3.4.1}
INPUT_DIR="/user/$(whoami)/sort-input"
LOCAL_FILE="input.txt"

echo "--- Generating test data for MapReduce Sort ---"

# 生成本地测试数据文件（可按需修改）
cat <<EOF > $LOCAL_FILE
orange
apple
banana
grape
kiwi
pear
mango
pineapple
lemon
strawberry
EOF

echo "✅ Created local file: $LOCAL_FILE"

# 上传前清理 HDFS 中旧目录
echo "📂 Creating HDFS directory: $INPUT_DIR"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash $INPUT_DIR > /dev/null 2>&1
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $INPUT_DIR

# 上传文件
echo "⏫ Uploading $LOCAL_FILE to HDFS: $INPUT_DIR"
$HADOOP_HOME/bin/hdfs dfs -put -f $LOCAL_FILE $INPUT_DIR/

# 验证上传结果
echo "📋 Verifying uploaded content:"
$HADOOP_HOME/bin/hdfs dfs -ls $INPUT_DIR
$HADOOP_HOME/bin/hdfs dfs -cat $INPUT_DIR/$LOCAL_FILE

echo "--- Upload Complete ---"
