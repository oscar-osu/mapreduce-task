package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceInsertIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Tracer tracer;
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();

    private String jobId, jobKind, jobSig;
    private String attemptIdStr, hostname = "unknown";
    private int partitionId;

    // --- 减速配置 ---
    private Set<Integer> slowReducePartitions = Collections.emptySet(); // 哪些分区命中
    private long slowReduceSleepMs = 0L;       // 每次 sleep 毫秒
    private int slowReduceEachN = 1;           // 每 N 个 key sleep 一次（持续减速）

    private String slowHeavyKey = null;        // 命中该 key 时 sleep
    private long slowHeavyKeySleepMs = 0L;

    private long processedGroups = 0L;         // 已处理 key 计数

    // 传递 traceparent 的 Getter
    private static final TextMapGetter<Configuration> GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Configuration c) {
            List<String> ks = new ArrayList<>();
            if (c.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + ".traceparent") != null) ks.add("traceparent");
            if (c.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + ".tracestate")  != null) ks.add("tracestate");
            return ks;
        }
        @Override public String get(Configuration c, String key) {
            return c.get(TraceInsertIndexJob.TRACE_CONTEXT_KEY + "." + key.toLowerCase());
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
        tracer = GlobalOpenTelemetry.getTracer("hadoop-insertindex-reducer", "1.0.0");

        jobId   = conf.get("trace.job.id");
        jobKind = conf.get("mr.job.kind", "insertindex");
        jobSig  = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId  = attemptID.getTaskID().getId();

        // 读取减速配置
        slowReducePartitions = parseIntSet(conf.get("insertindex.slow.reduce.partitions", ""));
        slowReduceSleepMs    = conf.getLong("insertindex.slow.reduce.sleep.ms", 0L);
        slowReduceEachN      = (int) conf.getLong("insertindex.slow.reduce.eachN", 1L);
        slowHeavyKey         = conf.get("insertindex.slow.reduce.heavy.key", null);
        slowHeavyKeySleepMs  = conf.getLong("insertindex.slow.reduce.heavy.sleep.ms", 0L);

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, GETTER);

        // 启动抖动（可选）：命中分区先 sleep 一次
        if (slowReduceSleepMs > 0 && slowReducePartitions.contains(partitionId)) {
            System.out.println("[SlowInject] setup: partition=" + partitionId +
                    " sleep " + slowReduceSleepMs + "ms");
            try { Thread.sleep(slowReduceSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }
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

        try (Scope s = span.makeCurrent()) {
            processedGroups++;

            // 重 key 减速
            if (slowHeavyKey != null && slowHeavyKeySleepMs > 0 && slowHeavyKey.equals(key.toString())) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", "reduce.heavykey.delay");
                span.setAttribute("mr.task.slow.key", slowHeavyKey);
                span.setAttribute("mr.task.slow.sleep.ms", slowHeavyKeySleepMs);
                context.getCounter("II", "SLOW_INJECT_HEAVY_KEY").increment(1);
                try { Thread.sleep(slowHeavyKeySleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

            // 分区级持续减速：每 N 个 key sleep 一次
            if (slowReduceSleepMs > 0
                    && slowReduceEachN > 0
                    && slowReducePartitions.contains(partitionId)
                    && (processedGroups % slowReduceEachN == 0)) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", "reduce.partition.eachN.delay");
                span.setAttribute("mr.task.slow.eachN", slowReduceEachN);
                span.setAttribute("mr.task.slow.sleep.ms", slowReduceSleepMs);
                context.getCounter("II", "SLOW_INJECT_PARTITION_EACHN_HITS").increment(1);
                try { Thread.sleep(slowReduceSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

            // —— 示例聚合逻辑（保持你原来的行为，去重后输出）——
            Set<String> docIds = new HashSet<>();
            for (Text v : values) {
                docIds.add(v.toString());
            }
            context.getCounter("INDEX", "KEYS_DISTINCT").increment(1);
            context.getCounter("INDEX", "UNIQUE_DOCIDS_OUT").increment(docIds.size());
            context.getCounter("INDEX", "REDUCER_LIST_EMIT").increment(1);

            StringBuilder sb = new StringBuilder();
            for (String id : docIds) {
                if (sb.length() > 0) sb.append(",");
                sb.append(id);
            }
            context.write(key, new Text(sb.toString()));

        } finally {
            long inRecs   = context.getCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            long inGroups = context.getCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();
            long outRecs  = context.getCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
            long shuffled = 0L;
            try { shuffled = context.getCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue(); } catch (Exception ignore) {}

            span.setAttribute("mr.reduce.input.records", inRecs);
            span.setAttribute("mr.reduce.input.groups",  inGroups);
            span.setAttribute("mr.reduce.output.records", outRecs);
            span.setAttribute("mr.reduce.shuffle.bytes", shuffled);
            span.end();
        }
    }
}
