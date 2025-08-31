#!/bin/bash

echo "--- Starting Clean Build for WordCount ---"

echo "Cleaning previous build artifacts..."
rm -rf classes/*
rm -f trace-wordcount-job.jar

# 设置 Hadoop 和 OpenTelemetry Classpath
HADOOP_HOME=${HADOOP_HOME:-/users/oscarz08/hadoop-3.4.1}
OTEL_LIB_DIR="./lib"

if [ ! -d "$HADOOP_HOME" ]; then
  echo "Error: HADOOP_HOME ($HADOOP_HOME) not found or not set."
  exit 1
fi

if [ ! -d "$OTEL_LIB_DIR" ] || [ -z "$(ls -A $OTEL_LIB_DIR/*.jar)" ]; then
  echo "Error: OTEL_LIB_DIR ($OTEL_LIB_DIR) not found or empty."
  exit 1
fi

# 构建 Hadoop Classpath
HADOOP_CLASSPATH=""
for jar in $(find $HADOOP_HOME -name "*.jar"); do
  HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$jar"
done

# 构建 OpenTelemetry Classpath
OTEL_CLASSPATH=""
for jar in $(find $OTEL_LIB_DIR -name "*.jar"); do
  OTEL_CLASSPATH="$OTEL_CLASSPATH:$jar"
done

# 最终编译 Classpath
COMPILE_CLASSPATH=".:$HADOOP_CLASSPATH:$OTEL_CLASSPATH"

# 创建 classes 目录 (如果不存在)
mkdir -p classes

# 编译 Java 文件
echo "Compiling Java source files..."
javac -Xlint:none -cp "$COMPILE_CLASSPATH" -d classes src/main/java/org/example/*.java

if [ $? -ne 0 ]; then
  echo "❌ Compilation failed!"
  exit 1
fi
echo "✅ Compilation successful."

# 打包 JAR 文件
echo "Creating Job JAR (trace-wordcount-job.jar) containing only job classes..."
jar cf trace-wordcount-job.jar -C classes .

if [ $? -ne 0 ]; then
  echo "❌ JAR creation failed!"
  exit 1
fi

echo "✅ Build finished successfully: trace-wordcount-job.jar created at $(ls -l trace-wordcount-job.jar | awk '{print $6, $7, $8}')"
echo "--- Clean Build Finished ---"
exit 0
