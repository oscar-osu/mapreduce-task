package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

public class TraceWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Tracer tracer;
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();
    private String jobId, jobKind, jobSig;
    private String attemptIdStr, hostname = "unknown";
    private int partitionId;

    // slowdown config（同名）
    private Set<Integer> slowReducePartitions = Collections.emptySet();
    private long slowReduceSleepMs = 0L;

    private String slowHeavyKey = null;
    private long slowHeavyKeySleepMs = 0L;

    // 让分区级注入在 setup 就生效（即使该分区无 key）
    private boolean slowPartitionConfigured = false;

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
        tracer = GlobalOpenTelemetry.getTracer("hadoop-wordcount-reducer", "1.0.0");
        jobId   = conf.get("trace.job.id");
        jobKind = conf.get("mr.job.kind", "unknown");
        jobSig  = conf.get("mr.job.signature.v1", "nosig");

        try { hostname = InetAddress.getLocalHost().getHostName(); } catch (Exception ignore) {}
        TaskAttemptID attemptID = context.getTaskAttemptID();
        attemptIdStr = attemptID.toString();
        partitionId  = attemptID.getTaskID().getId();

        slowReducePartitions = parseIntSet(conf.get("insertindex.slow.reduce.partitions", ""));
        slowReduceSleepMs    = conf.getLong("insertindex.slow.reduce.sleep.ms", 0L);
        slowHeavyKey         = conf.get("insertindex.slow.reduce.heavy.key", null);
        slowHeavyKeySleepMs  = conf.getLong("insertindex.slow.reduce.heavy.sleep.ms", 0L);

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, GETTER);

        // 分区级延迟在 setup 生效（即便本分区没有 key）
        if (slowReduceSleepMs > 0 && slowReducePartitions.contains(partitionId)) {
            slowPartitionConfigured = true;
            System.out.println("[SlowInject] reduce partition=" + partitionId +
                    " sleep " + slowReduceSleepMs + "ms at setup()");
            try { Thread.sleep(slowReduceSleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
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

            // “重 key” 注入：命中后在本 key 输出前 sleep
            if (slowHeavyKey != null && slowHeavyKeySleepMs > 0 && slowHeavyKey.equals(key.toString())) {
                span.setAttribute("mr.task.slow.injected", true);
                span.setAttribute("mr.task.slow.reason", "reduce.heavykey.delay");
                span.setAttribute("mr.task.slow.key", slowHeavyKey);
                span.setAttribute("mr.task.slow.sleep.ms", slowHeavyKeySleepMs);
                context.getCounter("WC", "SLOW_INJECT_HITS").increment(1);
                try { Thread.sleep(slowHeavyKeySleepMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }

            int sum = 0;
            for (IntWritable v : values) sum += v.get();
            context.write(key, new IntWritable(sum));

        } finally {
            long inRecs   = context.getCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            long inGroups = context.getCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();
            long outRecs  = context.getCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();

            long shuffledBytes = 0L;
            try { shuffledBytes = context.getCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue(); } catch (Exception ignore) {}

            long spillRecs = context.getCounter(TaskCounter.SPILLED_RECORDS).getValue();

            span.setAttribute("mr.reduce.input.records", inRecs);
            span.setAttribute("mr.reduce.input.groups",  inGroups);
            span.setAttribute("mr.reduce.output.records", outRecs);
            span.setAttribute("mr.reduce.shuffle.bytes", shuffledBytes);
            span.setAttribute("mr.reduce.spilled.records", spillRecs);

            span.end();
        }
    }
}
