package org.example;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TraceContextUtils {

    // ✅ 用于在 NameNode 提取 trace context
    public static class UgiTraceGetter implements TextMapGetter<UserGroupInformation> {
        @Override
        public Iterable<String> keys(UserGroupInformation carrier) {
            return () -> carrier.getTokens().stream()
                    .map(token -> new String(token.getIdentifier(), StandardCharsets.UTF_8))
                    .iterator();
        }

        @Override
        public String get(UserGroupInformation carrier, String key) {
            for (Token<?> token : carrier.getTokens()) {
                if (new String(token.getIdentifier(), StandardCharsets.UTF_8).equals(key)) {
                    return new String(token.getPassword(), StandardCharsets.UTF_8);
                }
            }
            return null;
        }
    }

    // ✅ 在 client / mapper / reducer 中注入 trace context 到 token
    public static void injectTraceContextToUgi() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        SpanContext ctx = Span.current().getSpanContext();

        if (!ctx.isValid()) {
            System.out.println("❌ SpanContext is invalid, skipping token injection.");
            return;
        }

        String traceparent = "00-" + ctx.getTraceId() + "-" + ctx.getSpanId() + "-01";

        System.out.println("✨ Injecting Token into UGI:");
        System.out.println("  Identifier: traceparent");
        System.out.println("  Password:   " + traceparent);

        Token<TokenIdentifier> traceToken = new Token<>(
                "traceparent".getBytes(StandardCharsets.UTF_8),
                traceparent.getBytes(StandardCharsets.UTF_8),
                new Text("tracecontext"),
                new Text("ugi")
        );

        ugi.addToken(traceToken);
        UserGroupInformation.setLoginUser(ugi);
    }

    // ✅ 在 mapper / reducer 中提取 trace context（用于 trace 关联）
    public static void extractTraceContextFromUgi() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        Map<String, String> carrier = new HashMap<>();

        for (Token<?> token : ugi.getTokens()) {
            if (new String(token.getIdentifier(), StandardCharsets.UTF_8).equals("traceparent")) {
                carrier.put("traceparent", new String(token.getPassword(), StandardCharsets.UTF_8));
            }
        }

        TextMapGetter<Map<String, String>> getter = new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(Map<String, String> carrier) {
                return carrier.keySet();
            }

            @Override
            public String get(Map<String, String> carrier, String key) {
                return carrier.get(key);
            }
        };

        Context extracted = GlobalOpenTelemetry.getPropagators()
                .getTextMapPropagator()
                .extract(Context.current(), carrier, getter);

        extracted.makeCurrent();
    }
}
