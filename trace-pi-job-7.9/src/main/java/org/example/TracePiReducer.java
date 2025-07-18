package org.example;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class TracePiReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    private Tracer tracer;
    private String jobId;
    private io.opentelemetry.context.Context parentContext = io.opentelemetry.context.Context.root();

    private static final TextMapGetter<Configuration> HADOOP_CONF_GETTER =
            new TextMapGetter<Configuration>() {
                @Override
                public Iterable<String> keys(Configuration carrier) {
                    Map<String, String> headers = new HashMap<>();
                    String traceParent = carrier.get(TracePiJob.TRACE_CONTEXT_KEY + ".traceparent");
                    if (traceParent != null) headers.put("traceparent", traceParent);
                    String traceState = carrier.get(TracePiJob.TRACE_CONTEXT_KEY + ".tracestate");
                    if (traceState != null) headers.put("tracestate", traceState);
                    return headers.keySet();
                }
                @Override
                public String get(Configuration carrier, String key) {
                    if (carrier == null) return null;
                    return carrier.get(TracePiJob.TRACE_CONTEXT_KEY + "." + key.toLowerCase());
                }
            };

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        tracer = GlobalOpenTelemetry.getTracer("hadoop-pi-reducer", "1.0.0");
        jobId = conf.get("trace.job.id");

        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, HADOOP_CONF_GETTER);

        try (Scope scope = parentContext.makeCurrent()) {
            TraceContextUtils.injectTraceContextToUgi();
        }

        UserGroupInformation.setLoginUser(UserGroupInformation.getCurrentUser());

        System.out.println("🌟 Mapper extracted Trace ID: " + Span.fromContext(parentContext).getSpanContext().getTraceId());

        System.out.println("Reducer extracted parent context and injected to UGI.");
    }


    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Span reducerSpan = tracer.spanBuilder("PiReduceTask")
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("reducer.task.id", context.getTaskAttemptID().toString())
                .setAttribute("input.key", key.get())
                .setAttribute("trace.job.id", jobId)
                .startSpan();

        try (Scope scope = reducerSpan.makeCurrent()) {

            TraceContextUtils.injectTraceContextToUgi();

            long sum = 0;
            int count = 0;
            for (LongWritable val : values) {
                sum += val.get();
                count++;
            }

            reducerSpan.setAttribute("values.count", count);
            reducerSpan.setAttribute("values.sum", sum);
            context.write(key, new LongWritable(sum));

        } finally {
            reducerSpan.end();
        }
    }
}
