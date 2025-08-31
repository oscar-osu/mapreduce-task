#!/bin/bash

INPUT_DIR="insertindex-input-parts"
HDFS_INPUT_PATH="/user/$(whoami)/insertindex-input"

echo "--- Preparing Insert Index Input Files ---"

# 清理本地旧目录并创建新的
rm -rf $INPUT_DIR
mkdir -p $INPUT_DIR

# 写入五个 part 文件，每个包含 3 个 word-docID 对
for i in {1..5}; do
  echo -e "apple\tdoc$i" > $INPUT_DIR/part-$i.txt
  echo -e "banana\tdoc$i" >> $INPUT_DIR/part-$i.txt
  echo -e "cat\tdoc$i" >> $INPUT_DIR/part-$i.txt
done

# 添加一个专门命中 reducer 0 的 key（如 zebra）
echo -e "zebra\tdoc6" > $INPUT_DIR/part-special.txt

echo "📄 Created input files locally in ./$INPUT_DIR"

# 上传到 HDFS
echo "🚀 Uploading to HDFS: $HDFS_INPUT_PATH"
hdfs dfs -rm -r -skipTrash $HDFS_INPUT_PATH > /dev/null 2>&1
hdfs dfs -mkdir -p $HDFS_INPUT_PATH
hdfs dfs -put $INPUT_DIR/* $HDFS_INPUT_PATH

# 预览上传内容
echo "✅ Uploaded successfully. Previewing HDFS content:"
hdfs dfs -cat $HDFS_INPUT_PATH/*

echo "--- Done ---"
