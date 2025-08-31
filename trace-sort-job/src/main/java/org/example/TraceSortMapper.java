package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceSortMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Tracer tracer;
    private String jobId;
    private String jobKind;
    private String jobSig;
    private String attemptIdStr;
    private int partitionId;
    private String hostname = "unknown";
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();

    // slowdown config（保持与 InsertIndex 一致的参数名）
    private Set<Integer> slowMapPartitions = Collections.emptySet();
    private long slowMapSleepMs = 0L;
    private int slowMapEachN = 1;
    private long recordCount = 0L;

    private boolean slowBySplit = false; // 可按路径注入（可选扩展）
    private String slowMapInputContains = null;

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
        tracer = GlobalOpenTelemetry.getTracer("hadoop-sort-mapper", "1.0.0");
        jobId = conf.get("trace.job.id");
        jobKind = conf.get("mr.job.kind", "unknown");
        jobSig = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId = attemptID.getTaskID().getId();

        // slowdown 配置（与 InsertIndex 保持一致的 key）
        slowMapPartitions = parseIntSet(conf.get("insertindex.slow.map.partitions", ""));
        slowMapSleepMs = conf.getLong("insertindex.slow.map.sleep.ms", 0L);
        slowMapEachN   = (int) conf.getLong("insertindex.slow.map.eachN", 1L);

        // 可选：按输入文件路径匹配注入（方便复现实验）
        slowMapInputContains = conf.get("insertindex.slow.map.input.contains", null);
        if (context.getInputSplit() instanceof FileSplit) {
            FileSplit split = (FileSplit) context.getInputSplit();
            String p = split.getPath().toString();
            if (slowMapInputContains != null && p.contains(slowMapInputContains)) {
                slowBySplit = true;
            }
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
            // Sort 的 map：把整行作为 key，value 空串
            String line = value.toString();
            context.write(new Text(line), new Text(""));
            context.getCounter("SORT", "RAW_LINES").increment(1);

            // —— 人为变慢：按 map 分区 + 每 N 条记录触发，或按 split 路径触发 ——
            recordCount++;
            boolean hit = slowMapSleepMs > 0
                    && (slowMapPartitions.contains(partitionId) || slowBySplit)
                    && (slowMapEachN <= 1 || (recordCount % slowMapEachN == 0));

            if (hit) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", slowBySplit ? "map.split.delay" : "map.partition.delay");
                span.setAttribute("mr.task.slow.sleep.ms", slowMapSleepMs);
                context.getCounter("SORT", "SLOW_INJECT_HITS").increment(1);
                try { Thread.sleep(slowMapSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

        } finally {
// === 记录输入体量，用于 ms/MB 归一化 ===
            long hdfsBytesRead = 0L;

// 1) 运行时读取通用计数器（不依赖枚举，兼容性最好）
            try {
                hdfsBytesRead = context.getCounter("FileSystemCounter", "HDFS_BYTES_READ").getValue();
            } catch (Exception ignore) {}

// 一些环境下走本地文件系统（可顺带尝试）
            if (hdfsBytesRead <= 0) {
                try {
                    hdfsBytesRead = context.getCounter("FileSystemCounter", "FILE_BYTES_READ").getValue();
                } catch (Exception ignore) {}
            }

// 2) 仍取不到就用 split 长度兜底（计划读取大小，近似）
            if (hdfsBytesRead <= 0 && context.getInputSplit() instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                org.apache.hadoop.mapreduce.lib.input.FileSplit split =
                        (org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit();
                hdfsBytesRead = split.getLength();
            }

// 记录到 span
            span.setAttribute("mr.map.input.hdfs.bytes", hdfsBytesRead);

// 这些计数器还可以保留（它们在 3.x 存在）
            long mapInputRecs = context.getCounter(org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS).getValue();
            span.setAttribute("mr.map.input.records", mapInputRecs);

            if (context.getInputSplit() instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                org.apache.hadoop.mapreduce.lib.input.FileSplit split =
                        (org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit();
                span.setAttribute("mr.map.input.split.path", split.getPath().toString());
                span.setAttribute("mr.map.input.split.length", split.getLength());
            }


            long outRecords = context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            long outBytes   = context.getCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
            long spillRecs  = context.getCounter(TaskCounter.SPILLED_RECORDS).getValue();
            span.setAttribute("mr.map.output.records", outRecords);
            span.setAttribute("mr.map.output.bytes", outBytes);
            span.setAttribute("mr.map.spilled.records", spillRecs);

            span.end();
        }
    }
}
