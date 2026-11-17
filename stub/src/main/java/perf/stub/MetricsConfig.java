package perf.stub;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MetricsConfig {

    @Bean
    MeterRegistryCustomizer<MeterRegistry> commonTags(@Value("${spring.application.name}") String app) {
        return registry -> registry.config().commonTags("application", app);
    }
}
