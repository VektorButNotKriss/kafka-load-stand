package perf.stub;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
public class UserIdListener {

    private static final Logger log = LoggerFactory.getLogger(UserIdListener.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final String outTopic;
    private final String suffix;

    private final Counter sent;
    private final Counter rejected;
    private final Counter sendErrors;

    public UserIdListener(KafkaTemplate<String, String> kafkaTemplate,
                          JsonMapper jsonMapper,
                          MeterRegistry registry,
                          @Value("${stub.topic.out}") String outTopic,
                          @Value("${stub.suffix}") String suffix) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.outTopic = outTopic;
        this.suffix = suffix;
        this.sent = counter(registry, "sent", "Messages successfully written to the output topic");
        this.rejected = counter(registry, "rejected", "Input messages with invalid JSON or empty userId");
        this.sendErrors = counter(registry, "send_error", "Messages that failed to be written to the output topic");
    }

    @KafkaListener(topics = "${stub.topic.in}")
    public void onMessage(ConsumerRecord<String, String> record) {
        UserMessage in = parse(record);
        if (in == null) {
            rejected.increment();
            return;
        }

        UserMessage out = new UserMessage(in.userId() + suffix);
        String payload = jsonMapper.writeValueAsString(out);

        kafkaTemplate.send(outTopic, record.key(), payload)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        sent.increment();
                    } else {
                        sendErrors.increment();
                        log.error("Failed to send userId={} to {}", out.userId(), outTopic, ex);
                    }
                });
    }

    private UserMessage parse(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            log.warn("Skip empty message, partition={} offset={}", record.partition(), record.offset());
            return null;
        }
        try {
            UserMessage msg = jsonMapper.readValue(record.value(), UserMessage.class);
            if (msg.userId() == null || msg.userId().isBlank()) {
                log.warn("Skip message without userId, offset={} value={}", record.offset(), record.value());
                return null;
            }
            return msg;
        } catch (JacksonException e) {
            log.warn("Skip invalid JSON, offset={} value={}", record.offset(), record.value());
            return null;
        }
    }

    private static Counter counter(MeterRegistry registry, String result, String description) {
        return Counter.builder("stub.messages")
                .tag("result", result)
                .description(description)
                .register(registry);
    }
}
