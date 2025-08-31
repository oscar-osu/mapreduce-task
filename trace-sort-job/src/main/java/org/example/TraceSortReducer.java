package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceSortReducer extends Reducer<Text, Text, Text, Text> {

    private Tracer tracer;
    private String jobId;
    private String jobKind;
    private String jobSig;
    private String attemptIdStr;
    private int partitionId;
    private String hostname = "unknown";
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();

    // slowdown config（与 InsertIndex 同名）
    private Set<Integer> slowReducePartitions = Collections.emptySet();
    private long slowReduceSleepMs = 0L;

    private String slowHeavyKey = null;
    private long slowHeavyKeySleepMs = 0L;

    private static final TextMapGetter<Configuration> HADOOP_CONF_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Configuration carrier) {
            Map<String, String> headers = new HashMap<>();
            String traceParent = carrier.get(TraceSortJob.TRACE_CONTEXT_KEY + ".traceparent");
            if (traceParent != null) headers.put("traceparent", traceParent);
            String traceState = carrier.get(TraceSortJob.TRACE_CONTEXT_KEY + ".tracestate");
            if (traceState != null) headers.put("tracestate", traceState);
            return headers.keySet();
        }
        @Override
        public String get(Configuration carrier, String key) {
            return carrier.get(TraceSortJob.TRACE_CONTEXT_KEY + "." + key.toLowerCase());
        }
    };

    private static Set<Integer> parseIntSet(String csv) {
        if (csv == null || csv.trim().isEmpty()) return Collections.emptySet();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        tracer = GlobalOpenTelemetry.getTracer("hadoop-sort-reducer", "1.0.0");
        jobId = conf.get("trace.job.id");
        jobKind = conf.get("mr.job.kind", "unknown");
        jobSig = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = java.net.InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId = attemptID.getTaskID().getId();

        slowReducePartitions = parseIntSet(conf.get("insertindex.slow.reduce.partitions", ""));
        slowReduceSleepMs    = conf.getLong("insertindex.slow.reduce.sleep.ms", 0L);

        slowHeavyKey         = conf.get("insertindex.slow.reduce.heavy.key", null);
        slowHeavyKeySleepMs  = conf.getLong("insertindex.slow.reduce.heavy.sleep.ms", 0L);

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, HADOOP_CONF_GETTER);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Span span = tracer.spanBuilder("mr.reduce")
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("trace.job.id", jobId)
                .setAttribute("mr.job.kind", jobKind)
                .setAttribute("mr.job.signature.v1", jobSig)
                .setAttribute("mr.task.kind", "reduce")
                .setAttribute("mr.task.attempt_id", attemptIdStr)
                .setAttribute("mr.task.partition", partitionId)
                .setAttribute("mr.task.hostname", hostname)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {

            // —— 人为变慢（1）：按 reduce 分区 ——
            if (slowReduceSleepMs > 0 && slowReducePartitions.contains(partitionId)) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", "reduce.partition.delay");
                span.setAttribute("mr.task.slow.sleep.ms", slowReduceSleepMs);
                context.getCounter("SORT", "SLOW_INJECT_HITS").increment(1);
                try { Thread.sleep(slowReduceSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

            // —— Sort 的 reduce：基本是 identity，把 key 原样输出 ——
            for (Text v : values) {
                context.write(key, v);
            }

            // —— 人为变慢（2）：针对某个“重 key” ——
            if (slowHeavyKey != null && slowHeavyKeySleepMs > 0 && slowHeavyKey.equals(key.toString())) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", "reduce.heavykey.delay");
                span.setAttribute("mr.task.slow.key", slowHeavyKey);
                span.setAttribute("mr.task.slow.sleep.ms", slowHeavyKeySleepMs);
                context.getCounter("SORT", "SLOW_INJECT_HITS").increment(1);
                try { Thread.sleep(slowHeavyKeySleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

        } finally {
            long reduceInRecs   = context.getCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            long reduceInGroups = context.getCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();
            long reduceOutRecs  = context.getCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
            long shuffledBytes  = context.getCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue();
            long spillRecs      = context.getCounter(TaskCounter.SPILLED_RECORDS).getValue();

            span.setAttribute("mr.reduce.input.records", reduceInRecs);
            span.setAttribute("mr.reduce.input.groups",  reduceInGroups);
            span.setAttribute("mr.reduce.output.records", reduceOutRecs);
            span.setAttribute("mr.reduce.shuffle.bytes", shuffledBytes);
            span.setAttribute("mr.reduce.spilled.records", spillRecs);

            span.end();
        }
    }
}
