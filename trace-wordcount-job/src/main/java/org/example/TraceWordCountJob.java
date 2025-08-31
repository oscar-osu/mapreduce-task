package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

public class TraceWordCountJob extends Configured implements Tool {

    public static final String TRACE_CONTEXT_KEY = "otel.trace.context";

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TraceWordCountJob <inputDir> <outputDir> [--reducers N]");
            return -1;
        }

        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        int reducers = 1;
        if (args.length >= 4 && "--reducers".equals(args[2])) {
            reducers = Integer.parseInt(args[3]);
        }

        Configuration conf = getConf();
        String jobId = "trace-wc-" + UUID.randomUUID();
        conf.set("trace.job.id", jobId);
        conf.set("mr.job.kind", "wordcount");

        Tracer tracer = GlobalOpenTelemetry.getTracer("hadoop-wordcount-client", "1.0.0");
        Span jobSpan = tracer.spanBuilder("mr.job.submit")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        int exitCode = -1;

        try (Scope scope = jobSpan.makeCurrent()) {
            // 把 traceparent 注入到 conf（后续携带到 Task）
            TextMapSetter<Configuration> setter = (carrier, key, value) -> {
                if (carrier != null) carrier.set(TRACE_CONTEXT_KEY + "." + key.toLowerCase(), value);
            };
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(io.opentelemetry.context.Context.current(), conf, setter);

            // 以 UGI Token 的方式再携带一份（与你现有链路保持一致）
            String traceparent = conf.get(TRACE_CONTEXT_KEY + ".traceparent", "");
            Token<?> traceToken = new Token<>(
                    "traceparent".getBytes(StandardCharsets.UTF_8),
                    traceparent.getBytes(StandardCharsets.UTF_8),
                    new Text("tracecontext"),
                    new Text("ugi")
            );

            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

            // 创建 Job
            Job job = Job.getInstance(conf, "Traceable WordCount");
            job.setJarByClass(TraceWordCountJob.class);
            job.getCredentials().addToken(new Text("traceparent"), traceToken);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapperClass(TraceWordCountMapper.class);
            job.setReducerClass(TraceWordCountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(reducers);

            TextInputFormat.setInputPaths(job, inputDir);
            TextOutputFormat.setOutputPath(job, outputDir);

            // ---- 结构签名（v1）----
            Configuration jc = job.getConfiguration(); // 重要：后续写入必须写到 jc
            String groupingComp = jc.get("mapreduce.job.output.group.comparator.class", "-");
            String sortComp     = jc.get("mapreduce.job.output.key.comparator.class", "-");
            String sigPlain = String.join("|",
                    job.getMapperClass().getName(),
                    job.getReducerClass() == null ? "-" : job.getReducerClass().getName(),
                    job.getCombinerClass() == null ? "-" : job.getCombinerClass().getName(),
                    job.getInputFormatClass().getName(),
                    job.getOutputFormatClass().getName(),
                    job.getMapOutputKeyClass().getName(),
                    job.getMapOutputValueClass().getName(),
                    job.getOutputKeyClass().getName(),
                    job.getOutputValueClass().getName(),
                    job.getPartitionerClass() == null ? "-" : job.getPartitionerClass().getName(),
                    groupingComp,
                    sortComp,
                    (job.getNumReduceTasks() == 0 ? "r0" : job.getNumReduceTasks() == 1 ? "r1" : "rmany"),
                    "sigv1"
            );
            String jobSignatureV1 = sha256Hex(sigPlain);

            // ✅ 关键：把签名写进 job 的配置（会分发到所有 Task）
            jc.set("mr.job.signature.v1", jobSignatureV1);
            // （可选，同步写回本地 conf，便于后续本进程读取）
            conf.set("mr.job.signature.v1", jobSignatureV1);

            // 根 span 属性
            jobSpan.setAttribute("trace.job.id", jobId);
            jobSpan.setAttribute("mr.job.kind", "wordcount");
            jobSpan.setAttribute("mr.job.signature.v1", jobSignatureV1);
            jobSpan.setAttribute("mr.mapper.class", job.getMapperClass().getName());
            jobSpan.setAttribute("mr.reducer.class", job.getReducerClass() == null ? "-" : job.getReducerClass().getName());
            jobSpan.setAttribute("mr.mapoutput.kv",
                    job.getMapOutputKeyClass().getName() + " / " + job.getMapOutputValueClass().getName());
            jobSpan.setAttribute("mr.output.kv",
                    job.getOutputKeyClass().getName() + " / " + job.getOutputValueClass().getName());
            jobSpan.setAttribute("mr.reducers.bucket",
                    (job.getNumReduceTasks() == 0 ? "0" : job.getNumReduceTasks() == 1 ? "1" : "many"));

            boolean success = ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> job.waitForCompletion(true));
            jobSpan.setAttribute("mapreduce.job.success", success);
            exitCode = success ? 0 : 1;
            if (!success) jobSpan.setStatus(StatusCode.ERROR, "Job failed");

        } catch (IOException | InterruptedException e) {
            jobSpan.recordException(e);
            jobSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            jobSpan.end();
        }

        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TraceWordCountJob(), args);
        System.exit(exitCode);
    }
}
