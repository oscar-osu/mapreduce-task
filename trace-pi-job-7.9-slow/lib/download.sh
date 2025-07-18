# 1. OpenTelemetry API (核心接口)
wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-api/1.37.0/opentelemetry-api-1.37.0.jar

# 2. OpenTelemetry Context (上下文管理)
wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-context/1.37.0/opentelemetry-context-1.37.0.jar

# 3. OpenTelemetry Trace Propagators Extension (包含 W3C Trace Context 支持)
wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-extension-trace-propagators/1.37.0/opentelemetry-extension-trace-propagators-1.37.0.jar

# (可选) 如果你的代码中用到了其他 OTel API 功能，可能需要对应的 JAR
# 例如，处理 Baggage:
# wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-api-baggage/1.37.0/opentelemetry-api-baggage-1.37.0.jar

wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-exporter-zipkin/1.37.0/opentelemetry-exporter-zipkin-1.37.0.jar

wget https://repo1.maven.org/maven2/io/opentelemetry/opentelemetry-sdk/1.37.0/opentelemetry-sdk-1.37.0.jar