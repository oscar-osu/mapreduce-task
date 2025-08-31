#!/bin/bash

HADOOP_HOME=${HADOOP_HOME:-/users/oscarz08/hadoop-3.4.1}
OTEL_AGENT_PATH="/users/oscarz08/opentelemetry-javaagent-2.14.0.jar"
OTEL_ZIPKIN_ENDPOINT="http://node0:9411/api/v2/spans"
OTEL_SERVICE_NAME="hadoop-sort-job-client"

INPUT_DIR="hdfs:///user/$(whoami)/sort-input"
OUTPUT_DIR="hdfs:///user/$(whoami)/sort-output-$(date +%s)"

echo "--- Running Dependency Checks ---"
if [ ! -d "$HADOOP_HOME" ]; then
  echo "Error: HADOOP_HOME ($HADOOP_HOME) not found or not set."
  exit 1
fi
if [ ! -f "$OTEL_AGENT_PATH" ]; then
  echo "Error: OpenTelemetry Java Agent not found at $OTEL_AGENT_PATH"
  exit 1
fi
if [ ! -f "trace-sort-job.jar" ]; then
  echo "Error: trace-sort-job.jar not found. Run build.sh first."
  exit 1
fi
if [ ! -d "lib" ] || [ -z "$(ls -A lib/opentelemetry-*.jar 2>/dev/null)" ]; then
    echo "Error: ./lib directory is empty or does not contain opentelemetry JARs."
    exit 1
fi

OTEL_JARS_CLASSPATH=""
while IFS= read -r -d $'\0' jarfile; do
    OTEL_JARS_CLASSPATH="${OTEL_JARS_CLASSPATH:+$OTEL_JARS_CLASSPATH:}$jarfile"
done < <(find ./lib -name 'opentelemetry-*.jar' -print0)

export HADOOP_CLASSPATH="${OTEL_JARS_CLASSPATH}:${HADOOP_CLASSPATH}"

echo "--- Job Configuration ---"
echo "  Input Dir: $INPUT_DIR"
echo "  Output Dir: $OUTPUT_DIR"
echo "---"

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS \
  -javaagent:${OTEL_AGENT_PATH} \
  -Dotel.service.name=${OTEL_SERVICE_NAME} \
  -Dotel.traces.exporter=zipkin \
  -Dotel.exporter.zipkin.endpoint=${OTEL_ZIPKIN_ENDPOINT} \
  -Dotel.metrics.exporter=none \
  -Dotel.logs.exporter=none \
  -Dotel.instrumentation.hadoop.enabled=true \
  -Dotel.propagators=tracecontext,baggage \
  -Dotel.baggage.trace.job.id=STATIC_SORT_JOB_ID \
  -Dotel.instrumentation.baggage.attributes=true"

echo "--- Runtime Environment ---"
echo "HADOOP_CLIENT_OPTS: $HADOOP_CLIENT_OPTS"
echo "Current HADOOP_CLASSPATH: $HADOOP_CLASSPATH"
echo "---"

echo "Attempting to remove previous output directory (if any): ${OUTPUT_DIR}"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash ${OUTPUT_DIR} > /dev/null 2>&1

OTEL_LIBJARS=$(find ./lib -name 'opentelemetry-*.jar' | paste -sd,)

echo "--- Submitting Sort Job to Hadoop ---"

#    变慢map
#    -D insertindex.slow.map.partitions=0,1 \
#    -D insertindex.slow.map.sleep.ms=2000 \
#    -D insertindex.slow.map.eachN=1 \

#    变慢reduce
#    -D mapreduce.job.reduces=5 \
#    -D insertindex.slow.reduce.partitions=0,1,2,3,4 \
#    -D insertindex.slow.reduce.sleep.ms=1200 \

$HADOOP_HOME/bin/hadoop jar trace-sort-job.jar \
    org.example.TraceSortJob \
    -libjars "$OTEL_LIBJARS" \
    -D insertindex.slow.map.partitions=0,1 \    # 插入位置
    -D insertindex.slow.map.sleep.ms=2000 \     # 插入位置
    -D insertindex.slow.map.eachN=1 \           # 插入位置
    -D mapreduce.map.java.opts="-javaagent:${OTEL_AGENT_PATH} -Dotel.service.name=hadoop-sort-map -Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=${OTEL_ZIPKIN_ENDPOINT} -Dotel.metrics.exporter=none -Dotel.logs.exporter=none -Dotel.propagators=tracecontext,baggage" \
    -D mapreduce.reduce.java.opts="-javaagent:${OTEL_AGENT_PATH} -Dotel.service.name=hadoop-sort-reduce -Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=${OTEL_ZIPKIN_ENDPOINT} -Dotel.metrics.exporter=none -Dotel.logs.exporter=none -Dotel.propagators=tracecontext,baggage" \
    $INPUT_DIR \
    $OUTPUT_DIR

JOB_EXIT_CODE=$?
echo "--- Job Execution Finished ---"

if [ $JOB_EXIT_CODE -eq 0 ]; then
  echo "Job completed successfully."
  echo "Output is in HDFS: ${OUTPUT_DIR}"
else
  echo "Job failed with exit code $JOB_EXIT_CODE."
fi

exit $JOB_EXIT_CODE
