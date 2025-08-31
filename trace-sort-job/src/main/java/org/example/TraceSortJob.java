package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
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

public class TraceSortJob extends Configured implements Tool {

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
            System.err.println("Usage: TraceSortJob <inputDir> <outputDir> [--reducers N]");
            return -1;
        }

        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        int reducers = 1; // 默认 1，支持覆盖
        if (args.length >= 4 && "--reducers".equals(args[2])) {
            reducers = Integer.parseInt(args[3]);
        }

        Configuration conf = getConf();
        String jobId = "trace-sort-" + UUID.randomUUID();
        conf.set("trace.job.id", jobId);
        conf.set("mr.job.kind", "sort");

        System.out.println("Generated trace.job.id: " + jobId);

        Tracer tracer = GlobalOpenTelemetry.getTracer("hadoop-sort-job-client", "1.0.0");
        Span jobSpan = tracer.spanBuilder("mr.job.submit")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        int exitCode = -1;

        try (Scope scope = jobSpan.makeCurrent()) {
            final TextMapSetter<Configuration> hadoopConfSetter =
                    (carrier, key, value) -> {
                        if (carrier != null) {
                            carrier.set(TRACE_CONTEXT_KEY + "." + key.toLowerCase(), value);
                        }
                    };

            // 注入 OTel 上下文
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(io.opentelemetry.context.Context.current(), conf, hadoopConfSetter);

            // 通过 UGI Token 携带 traceparent
            Token<?> traceToken = new Token<>(
                    "traceparent".getBytes(StandardCharsets.UTF_8),
                    conf.get(TRACE_CONTEXT_KEY + ".traceparent", "").getBytes(StandardCharsets.UTF_8),
                    new Text("tracecontext"),
                    new Text("ugi")
            );

            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            Job job = Job.getInstance(conf, "Traceable Hadoop Sort Job");
            job.setJarByClass(TraceSortJob.class);
            job.getCredentials().addToken(new Text("traceparent"), traceToken);

            // IO & MR 类
            job.setInputFormatClass(TextInputFormat.class);
            // 小文件多时可切换为 CombineTextInputFormat：
            // job.setInputFormatClass(CombineTextInputFormat.class);
            // CombineTextInputFormat.setMaxInputSplitSize(job, 134217728L);
            // CombineTextInputFormat.setMinInputSplitSize(job, 67108864L);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapperClass(TraceSortMapper.class);
            job.setReducerClass(TraceSortReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(reducers);

            TextInputFormat.setInputPaths(job, inputDir);
            TextOutputFormat.setOutputPath(job, outputDir);

            // ---- 构造“结构签名”并写入 job 配置 + 根 span ----
            Configuration jc = job.getConfiguration(); // 关键：签名写入 jc，才能下发到 Task
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

            // ✅ 写入 job 的配置（会随 Job 分发到 Mapper/Reducer）
            jc.set("mr.job.signature.v1", jobSignatureV1);
            // （可选）本地 conf 也写一份
            conf.set("mr.job.signature.v1", jobSignatureV1);

            // 根 span 属性
            jobSpan.setAttribute("trace.job.id", jobId);
            jobSpan.setAttribute("mr.job.kind", "sort");
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
            exitCode = success ? 0 : 1;
            jobSpan.setAttribute("mapreduce.job.success", success);
            if (!success) jobSpan.setStatus(StatusCode.ERROR, "Job failed");
        } catch (IOException | InterruptedException e) {
            jobSpan.recordException(e);
            jobSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            jobSpan.end();
            System.out.println("Sort job finished with exit code: " + exitCode);
        }

        return exitCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TraceSortJob(), args);
        System.exit(exitCode);
    }
}
