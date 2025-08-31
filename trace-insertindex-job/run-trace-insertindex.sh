#!/bin/bash

# 设置输出目录带时间戳
TIMESTAMP=$(date +%s)
INPUT_PATH="/user/$(whoami)/insertindex-input"
OUTPUT_PATH="/user/$(whoami)/insertindex-output-$TIMESTAMP"
MAIN_CLASS="org.example.TraceInsertIndexJob"
JAR_NAME="trace-insertindex-job.jar"

# 构造 -libjars 参数
OTEL_JARS=$(echo lib/*.jar | tr ' ' ',')
if [ -z "$OTEL_JARS" ]; then
  echo "❌ Error: No JARs found in ./lib directory. Make sure OpenTelemetry JARs are there."
  exit 1
fi

# 删除输出目录（如已存在）
hdfs dfs -rm -r -skipTrash $OUTPUT_PATH > /dev/null 2>&1

# 运行 MapReduce 作业
echo "--- Running Insert Index Job ---"
echo "Input:  $INPUT_PATH"
echo "Output: $OUTPUT_PATH"
echo

hadoop jar $JAR_NAME $MAIN_CLASS \
  $INPUT_PATH $OUTPUT_PATH \
  -libjars $OTEL_JARS

STATUS=$?

if [ $STATUS -eq 0 ]; then
  echo "✅ Job completed successfully."
  echo "--- Output Preview (part-r-*) ---"
  hdfs dfs -cat $OUTPUT_PATH/part-r-* | head -n 20
else
  echo "❌ Job failed with status code $STATUS"
fi
