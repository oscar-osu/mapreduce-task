package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private final Text word = new Text();

    private Tracer tracer;
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();
    private String jobId, jobKind, jobSig;
    private String attemptIdStr, hostname = "unknown";
    private int partitionId;

    // slowdown config（与 InsertIndex/Sort 保持一致）
    private Set<Integer> slowMapPartitions = Collections.emptySet();
    private long slowMapSleepMs = 0L;
    private int slowMapEachN = 1;
    private long recordCount = 0L;

    private boolean slowBySplit = false;
    private String slowMapInputContains = null;

    private static final TextMapGetter<Configuration> GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Configuration c) {
            Map<String,String> m = new HashMap<>();
            String tp = c.get(TraceWordCountJob.TRACE_CONTEXT_KEY + ".traceparent");
            if (tp != null) m.put("traceparent", tp);
            String ts = c.get(TraceWordCountJob.TRACE_CONTEXT_KEY + ".tracestate");
            if (ts != null) m.put("tracestate", ts);
            return m.keySet();
        }
        @Override public String get(Configuration c, String key) {
            return c.get(TraceWordCountJob.TRACE_CONTEXT_KEY + "." + key.toLowerCase());
        }
    };

    private static Set<Integer> parseIntSet(String csv) {
        if (csv == null || csv.trim().isEmpty()) return Collections.emptySet();
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .map(Integer::parseInt).collect(Collectors.toSet());
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        tracer = GlobalOpenTelemetry.getTracer("hadoop-wordcount-mapper", "1.0.0");
        jobId   = conf.get("trace.job.id");
        jobKind = conf.get("mr.job.kind", "unknown");
        jobSig  = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId  = attemptID.getTaskID().getId();

        slowMapPartitions = parseIntSet(conf.get("insertindex.slow.map.partitions", ""));
        slowMapSleepMs = conf.getLong("insertindex.slow.map.sleep.ms", 0L);
        slowMapEachN   = (int) conf.getLong("insertindex.slow.map.eachN", 1L);

        slowMapInputContains = conf.get("insertindex.slow.map.input.contains", null);
        if (context.getInputSplit() instanceof FileSplit) {
            FileSplit split = (FileSplit) context.getInputSplit();
            String p = split.getPath().toString();
            if (slowMapInputContains != null && p.contains(slowMapInputContains)) slowBySplit = true;
        }

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, GETTER);
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
            String line = value.toString();
            // 简单分词（仅字母），与原逻辑一致
            String[] toks = line.split("\\s+");
            for (String t : toks) {
                String token = t.toLowerCase().replaceAll("[^a-z]", "");
                if (!token.isEmpty()) {
                    word.set(token);
                    context.write(word, ONE);
                    recordCount++;
                }

                boolean hit = slowMapSleepMs > 0
                        && (slowMapPartitions.contains(partitionId) || slowBySplit)
                        && (slowMapEachN <= 1 || (recordCount % slowMapEachN == 0));
                if (hit) {
                    span.setAttribute("mr.task.slow.injected", true);
                    span.setAttribute("mr.task.slow.reason", slowBySplit ? "map.split.delay" : "map.partition.delay");
                    span.setAttribute("mr.task.slow.sleep.ms", slowMapSleepMs);
                    context.getCounter("WC", "SLOW_INJECT_HITS").increment(1);
                    try { Thread.sleep(slowMapSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        } finally {
            // 输入体量：先尝试通用计数器，再用 split 长度兜底
            long hdfsBytesRead = 0L;
            try { hdfsBytesRead = context.getCounter("FileSystemCounter", "HDFS_BYTES_READ").getValue(); } catch (Exception ignore) {}
            if (hdfsBytesRead <= 0) {
                try { hdfsBytesRead = context.getCounter("FileSystemCounter", "FILE_BYTES_READ").getValue(); } catch (Exception ignore) {}
            }
            if (hdfsBytesRead <= 0 && context.getInputSplit() instanceof FileSplit) {
                FileSplit split = (FileSplit) context.getInputSplit();
                hdfsBytesRead = split.getLength();
            }
            span.setAttribute("mr.map.input.hdfs.bytes", hdfsBytesRead);

            long inRecs  = context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
            long outRecs = context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            long outBytes= context.getCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
            long spillRecs = context.getCounter(TaskCounter.SPILLED_RECORDS).getValue();

            span.setAttribute("mr.map.input.records", inRecs);
            span.setAttribute("mr.map.output.records", outRecs);
            span.setAttribute("mr.map.output.bytes", outBytes);
            span.setAttribute("mr.map.spilled.records", spillRecs);

            if (context.getInputSplit() instanceof FileSplit) {
                FileSplit split = (FileSplit) context.getInputSplit();
                span.setAttribute("mr.map.input.split.path", split.getPath().toString());
                span.setAttribute("mr.map.input.split.length", split.getLength());
            }

            span.end();
        }
    }
}
