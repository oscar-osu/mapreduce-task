#!/bin/bash

HADOOP_HOME=${HADOOP_HOME:-/users/oscarz08/hadoop-3.4.1}
OTEL_AGENT_PATH="/users/oscarz08/opentelemetry-javaagent-2.14.0.jar"
OTEL_ZIPKIN_ENDPOINT="http://node0:9411/api/v2/spans"
OTEL_SERVICE_NAME="hadoop-pi-job-client"

NUM_MAPS=5
POINTS_PER_MAP=100000

#export CUSTOM_TRACE_ID=$(uuidgen | tr -d '-' | cut -c1-32)

echo "--- Running Dependency Checks ---"
if [ ! -d "$HADOOP_HOME" ]; then
  echo "Error: HADOOP_HOME ($HADOOP_HOME) not found or not set."
  exit 1
fi
if [ ! -f "$OTEL_AGENT_PATH" ]; then
  echo "Error: OpenTelemetry Java Agent not found at $OTEL_AGENT_PATH"
  exit 1
fi
if [ ! -f "trace-pi-job.jar" ]; then
  echo "Error: trace-pi-job.jar not found. Run build.sh first."
  exit 1
fi
if [ ! -d "lib" ] || [ -z "$(ls -A lib/opentelemetry-*.jar 2>/dev/null)" ]; then
    echo "Error: ./lib directory is empty or does not contain opentelemetry JARs. Download OTel API JARs first."
    exit 1
fi
echo "Dependency checks passed."
echo "---"

OTEL_JARS_CLASSPATH=""
echo "Building classpath string from JARs in ./lib ..."

while IFS= read -r -d $'\0' jarfile; do
    if [ -z "$OTEL_JARS_CLASSPATH" ]; then
        OTEL_JARS_CLASSPATH="$jarfile"
    else
        OTEL_JARS_CLASSPATH="$OTEL_JARS_CLASSPATH:$jarfile"
    fi
done < <(find ./lib -name 'opentelemetry-*.jar' -print0)


if [ -z "$OTEL_JARS_CLASSPATH" ]; then
    echo "Error: Could not find any opentelemetry JARs in ./lib"
    exit 1
fi

echo "Adding OTel JARs to HADOOP_CLASSPATH for client: $OTEL_JARS_CLASSPATH"

export HADOOP_CLASSPATH="${OTEL_JARS_CLASSPATH}:${HADOOP_CLASSPATH}"

OUTPUT_DIR="hdfs:///user/$(whoami)/pi-output-$(date +%s)"

echo "--- Job Configuration ---"
echo "Running Pi Job:"
echo "  Num Maps: $NUM_MAPS"
echo "  Points Per Map: $POINTS_PER_MAP"
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
  -Dotel.baggage.trace.job.id=STATIC_TEST_JOB_ID \
  -Dotel.instrumentation.baggage.attributes=true"

echo "--- Runtime Environment ---"
echo "HADOOP_CLIENT_OPTS: $HADOOP_CLIENT_OPTS"
echo "Current HADOOP_CLASSPATH: $HADOOP_CLASSPATH"
echo "---"

echo "Attempting to remove previous output directory (if any): ${OUTPUT_DIR}"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash ${OUTPUT_DIR} > /dev/null 2>&1

OTEL_LIBJARS=$(find ./lib -name 'opentelemetry-*.jar' | paste -sd,)
echo "Using libjars for task distribution: $OTEL_LIBJARS"

echo "--- Submitting Job to Hadoop ---"

$HADOOP_HOME/bin/hadoop jar trace-pi-job.jar \
    org.example.TracePiJob \
    -libjars "$OTEL_LIBJARS" \
    -D mapreduce.map.java.opts="-javaagent:${OTEL_AGENT_PATH} -Dotel.service.name=hadoop-pi-map -Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=${OTEL_ZIPKIN_ENDPOINT} -Dotel.metrics.exporter=none -Dotel.logs.exporter=none -Dotel.propagators=tracecontext,baggage" \
    -D mapreduce.reduce.java.opts="-javaagent:${OTEL_AGENT_PATH} -Dotel.service.name=hadoop-pi-reduce -Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=${OTEL_ZIPKIN_ENDPOINT} -Dotel.metrics.exporter=none -Dotel.logs.exporter=none -Dotel.propagators=tracecontext,baggage" \
    $NUM_MAPS \
    $POINTS_PER_MAP \
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