package org.example;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TraceContextUtils {

    // ✅ 用于在 NameNode 提取 trace context（你已经在 NameNodeRpcServer 用了）
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

        // traceparent format: 00-<traceId>-<spanId>-01
        String traceparent = "00-" + ctx.getTraceId() + "-" + ctx.getSpanId() + "-01";

        System.out.println("✨ Injecting Token into UGI:");
        System.out.println("  Identifier: traceparent");
        System.out.println("  Password:   " + traceparent);

        Token<TokenIdentifier> traceToken = new Token<>(
                "traceparent".getBytes(StandardCharsets.UTF_8),     // identifier
                traceparent.getBytes(StandardCharsets.UTF_8),       // password
                new Text("tracecontext"),                           // kind
                new Text("ugi")                                     // service
        );

        ugi.addToken(traceToken);

        // 🔐 设置当前线程的登录用户（必须）
        UserGroupInformation.setLoginUser(ugi);
    }
}
