package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceInsertIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Tracer tracer;
    private String jobId, jobKind, jobSig;
    private String attemptIdStr, hostname = "unknown";
    private int partitionId;
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();

    // --- 减速配置 ---
    private Set<Integer> slowMapPartitions = Collections.emptySet(); // 哪些 map 分区命中
    private long slowMapSleepMs = 0L;         // 每次 sleep 毫秒
    private int  slowMapEachN   = 1;          // 每 N 条记录 sleep 一次
    private String slowMapInputContains = null; // 输入 split 路径包含则命中
    private boolean slowBySplit = false;      // 当前 split 是否命中路径规则
    private long recordCount = 0L;            // 已处理记录数

    private static final TextMapGetter<Configuration> HADOOP_CONF_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Configuration carrier) {
            List<String> ks = new ArrayList<>();
            if (carrier.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + ".traceparent") != null) ks.add("traceparent");
            if (carrier.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + ".tracestate")  != null) ks.add("tracestate");
            return ks;
        }
        @Override
        public String get(Configuration carrier, String key) {
            return carrier.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + "." + key.toLowerCase());
        }
    };

    private static Set<Integer> parseIntSet(String csv) {
        if (csv == null || csv.trim().isEmpty()) return Collections.emptySet();
        return Arrays.stream(csv.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(Integer::parseInt).collect(Collectors.toSet());
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        tracer = GlobalOpenTelemetry.getTracer("hadoop-insert-index-mapper", "1.0.0");

        jobId  = conf.get("trace.job.id");
        jobKind= conf.get("mr.job.kind", "unknown");
        jobSig = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId  = attemptID.getTaskID().getId();

        // 读取减速配置
        slowMapPartitions     = parseIntSet(conf.get("insertindex.slow.map.partitions", ""));
        slowMapSleepMs        = conf.getLong("insertindex.slow.map.sleep.ms", 0L);
        slowMapEachN          = (int) conf.getLong("insertindex.slow.map.eachN", 1L);
        slowMapInputContains  = conf.get("insertindex.slow.map.input.contains", null);

        // 判断当前 split 是否命中路径规则
        if (context.getInputSplit() instanceof FileSplit) {
            String p = ((FileSplit) context.getInputSplit()).getPath().toString();
            slowBySplit = (slowMapInputContains != null && p.contains(slowMapInputContains));
        }

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, HADOOP_CONF_GETTER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Span span = tracer.spanBuilder("mr.map")
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("trace.job.id", jobId)
                .setAttribute("mr.job.kind", jobKind)
                .setAttribute("mr.job.signature.v1", jobSig)
                .setAttribute("mr.task.kind", "map")
                .setAttribute("mr.task.attempt_id", attemptIdStr)
                .setAttribute("mr.task.partition", partitionId)
                .setAttribute("mr.task.hostname", hostname)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                context.getCounter("INDEX", "MALFORMED_LINES").increment(1);
                return;
            }

            String[] parts = line.split("\t", 2);
            if (parts.length == 2) {
                context.write(new Text(parts[0]), new Text(parts[1]));
                context.getCounter("INDEX", "RAW_PAIRS_IN").increment(1);
            } else {
                context.getCounter("INDEX", "MALFORMED_LINES").increment(1);
            }

            // —— 减速触发：分区或路径命中 + eachN —— //
            recordCount++;
            boolean partitionHit = slowMapPartitions.contains(partitionId);
            boolean shouldSleep =
                    slowMapSleepMs > 0 &&
                            slowMapEachN   > 0 &&
                            (partitionHit || slowBySplit) &&
                            (recordCount % slowMapEachN == 0);

            if (shouldSleep) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", slowBySplit ? "map.split.delay" : "map.partition.eachN.delay");
                span.setAttribute("mr.task.slow.eachN", slowMapEachN);
                span.setAttribute("mr.task.slow.sleep.ms", slowMapSleepMs);
                context.getCounter("II", "SLOW_INJECT_MAP_HITS").increment(1);
                try { Thread.sleep(slowMapSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

        } finally {
            // 常用 TaskCounter
            long outRecords = context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            long outBytes   = context.getCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
            long spillRecs  = context.getCounter(TaskCounter.SPILLED_RECORDS).getValue();

            span.setAttribute("mr.map.output.records", outRecords);
            span.setAttribute("mr.map.output.bytes", outBytes);
            span.setAttribute("mr.map.spilled.records", spillRecs);

            // split 信息（便于排查）
            if (context.getInputSplit() instanceof FileSplit) {
                FileSplit split = (FileSplit) context.getInputSplit();
                span.setAttribute("mr.map.input.split.path", split.getPath().toString());
                span.setAttribute("mr.map.input.split.length", split.getLength());
            }

            span.end();
        }
    }
}
