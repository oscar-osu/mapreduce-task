package org.example;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TracePiMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

    public static final String NUM_MAPS_KEY = "pi.estimator.num.maps";
    public static final String NUM_POINTS_KEY = "pi.estimator.num.points";

    private long numPointsToGenerate;
    private final Random random = new Random();
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
        numPointsToGenerate = conf.getLong(NUM_POINTS_KEY, 1000);
        tracer = GlobalOpenTelemetry.getTracer("hadoop-pi-mapper", "1.0.0");
        jobId = conf.get("trace.job.id");

        // Extract context from conf
        parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(io.opentelemetry.context.Context.current(), conf, HADOOP_CONF_GETTER);

        // Inject as token to UGI
        String traceparent = conf.get(TracePiJob.TRACE_CONTEXT_KEY + ".traceparent");
        if (traceparent != null) {
            Token<?> token = new Token<>(
                    "traceparent".getBytes(StandardCharsets.UTF_8),
                    traceparent.getBytes(StandardCharsets.UTF_8),
                    new Text("tracecontext"),
                    new Text("ugi")
            );
            UserGroupInformation.getCurrentUser().addToken(token);
        }

        UserGroupInformation.setLoginUser(UserGroupInformation.getCurrentUser());

        System.out.println("🌟 Mapper extracted Trace ID: " + Span.fromContext(parentContext).getSpanContext().getTraceId());
        System.out.println("Mapper injected trace token into UGI.");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Span mapperSpan = tracer.spanBuilder("PiMapTask")
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("mapper.task.id", context.getTaskAttemptID().toString())
                .setAttribute("points.to.generate", numPointsToGenerate)
                .setAttribute("trace.job.id", jobId)
                .startSpan();

        System.out.println("Mapper: Java Agent span started: " + mapperSpan.getSpanContext().getTraceId());

        try (Scope scope = mapperSpan.makeCurrent()) {

            final long[] pointsInsideHolder = new long[]{0};

            Span calculationSpan = tracer.spanBuilder("GenerateAndCheckPoints")
                    .setParent(io.opentelemetry.context.Context.current())
                    .startSpan();

            try (Scope calcScope = calculationSpan.makeCurrent()) {
                for (long i = 0; i < numPointsToGenerate; ++i) {
                    double x = random.nextDouble();
                    double y = random.nextDouble();
                    if (x * x + y * y <= 1.0) {
                        pointsInsideHolder[0]++;
                    }
                }
                calculationSpan.setAttribute("points.generated", numPointsToGenerate);
                calculationSpan.setAttribute("points.inside.circle", pointsInsideHolder[0]);
            } finally {
                calculationSpan.end();
            }

            final long pointsInside = pointsInsideHolder[0];

            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                context.write(new LongWritable(1L), new LongWritable(numPointsToGenerate));
                context.write(new LongWritable(2L), new LongWritable(pointsInside));
                return null;
            });

            mapperSpan.addEvent("Finished HDFS.write");

        } finally {
            mapperSpan.end();
        }
    }
}
