package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Random;
import java.util.UUID;

public class TracePiJob extends Configured implements Tool {

    public static final String TRACE_CONTEXT_KEY = "otel.trace.context";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TracePiJob <numMaps> <numPointsPerMap> <outputDir>");
            return -1;
        }

        int numMaps = Integer.parseInt(args[0]);
        long numPoints = Long.parseLong(args[1]);
        Path outputDir = new Path(args[2]);

        Configuration conf = getConf();
        String jobId = "trace-job-" + UUID.randomUUID();
        conf.set("trace.job.id", jobId);
        System.out.println("Generated trace.job.id: " + jobId);

        Tracer tracer = GlobalOpenTelemetry.getTracer("hadoop-pi-job-client", "1.0.0");

        Span jobSpan = tracer.spanBuilder("PiJobSubmit")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        System.out.println("TRACE ID: " + jobSpan.getSpanContext().getTraceId());
        jobSpan.setAttribute("trace.job.id", jobId);

        int exitCode = -1;

        try (Scope scope = jobSpan.makeCurrent()) {
            conf.setInt(TracePiMapper.NUM_MAPS_KEY, numMaps);
            conf.setLong(TracePiMapper.NUM_POINTS_KEY, numPoints);

            final TextMapSetter<Configuration> hadoopConfSetter =
                    (carrier, key, value) -> {
                        if (carrier != null) {
                            carrier.set(TRACE_CONTEXT_KEY + "." + key.toLowerCase(), value);
                        }
                    };

            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(io.opentelemetry.context.Context.current(), conf, hadoopConfSetter);

            System.out.println("Injected Trace Context into Hadoop Configuration:");
            System.out.println("  TraceParent: " + conf.get(TRACE_CONTEXT_KEY + ".traceparent"));
            System.out.println("  TraceState: " + conf.get(TRACE_CONTEXT_KEY + ".tracestate"));

            TraceContextUtils.injectTraceContextToUgi();

            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            System.out.println("👀 Client UGI Tokens:");
            for (Token<?> token : ugi.getTokens()) {
                System.out.println("🚚 Token Kind: " + token.getKind() +
                        ", Service: " + token.getService() +
                        ", ID: " + new String(token.getIdentifier()));
            }

            Job job = Job.getInstance(conf, "Traceable Hadoop Pi Estimator");
            job.setJarByClass(TracePiJob.class);

            // ✅ 将 trace token 加进 Job credentials（这是关键补充）
            Token<?> traceToken = new Token<>(
                    "traceparent".getBytes(StandardCharsets.UTF_8),
                    conf.get(TRACE_CONTEXT_KEY + ".traceparent").getBytes(StandardCharsets.UTF_8),
                    new Text("tracecontext"),
                    new Text("ugi")
            );
            job.getCredentials().addToken(new Text("traceparent"), traceToken);

            Path inputDir = new Path(outputDir.getParent(), "pi-input-" + new Random().nextInt());

            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                org.apache.hadoop.fs.FileSystem fs = inputDir.getFileSystem(conf);
                fs.mkdirs(inputDir.getParent());
                fs.mkdirs(inputDir);
                try (FSDataOutputStream out = fs.create(new Path(inputDir, "dummy.txt"))) {
                    for (int i = 0; i < numMaps; i++) {
                        out.writeUTF("input line " + i + "\n");
                    }
                }
                return null;
            });

            FileInputFormat.setInputPaths(job, inputDir);
            job.setMapperClass(TracePiMapper.class);
            job.setReducerClass(TracePiReducer.class);
            job.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.setNumLinesPerSplit(job, 1);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);
            FileOutputFormat.setOutputPath(job, outputDir);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setNumReduceTasks(1);

            jobSpan.setAttribute("mapreduce.job.name", job.getJobName());
            jobSpan.setAttribute("mapreduce.num.maps", numMaps);
            jobSpan.setAttribute("mapreduce.num.points.per.map", numPoints);
            jobSpan.setAttribute("mapreduce.output.dir", outputDir.toString());

            System.out.println("Submitting Pi job...");
            boolean success = ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> job.waitForCompletion(true));
            exitCode = success ? 0 : 1;
            jobSpan.setAttribute("mapreduce.job.success", success);

            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                org.apache.hadoop.fs.FileSystem fs = inputDir.getFileSystem(conf);
                if (fs.exists(inputDir)) {
                    fs.delete(inputDir, true);
                }
                return null;
            });

        } catch (IOException e) {
            jobSpan.recordException(e);
            jobSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            jobSpan.recordException(e);
            jobSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            jobSpan.end();
            System.out.println("Pi job finished with exit code: " + exitCode);
        }

        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting TracePiJob client...");
        int exitCode = ToolRunner.run(new TracePiJob(), args);
        System.exit(exitCode);
    }
}
