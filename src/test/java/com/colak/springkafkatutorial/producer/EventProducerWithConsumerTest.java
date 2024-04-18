
package com.colak.springkafkatutorial.producer;

import com.colak.kafkapublishertest.avro.MyEventAvro;
import com.colak.kafkapublishertest.avro.MyKeyAvro;
import com.colak.springkafkatutorial.model.MyEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@SpringBootTest
@Testcontainers
class EventProducerWithConsumerTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
                    .withKraft() // To not have to start Zookeeper
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true") // To allow auto creation of topics
                    .withNetwork(NETWORK);

    @Container
    @SuppressWarnings("resource")
    private static final GenericContainer<?> REGISTRY_CONTAINER =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:latest"))
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv(
                            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            STR."PLAINTEXT://\{KAFKA_CONTAINER.getNetworkAliases().getFirst()}:9092");

    static {
        KAFKA_CONTAINER.start();
        REGISTRY_CONTAINER.start();
    }

    @Autowired
    private EventProducer eventProducer;


    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", EventProducerWithConsumerTest::getSchemaRegistryUrl);
    }

    private static String getSchemaRegistryUrl() {
        return String.format(
                "http://%s:%s", REGISTRY_CONTAINER.getHost(), REGISTRY_CONTAINER.getMappedPort(8081));
    }

    @Test
    void testWithConsumer() {
        // given
        MyEvent event = new MyEvent(UUID.randomUUID(), 1, LocalDateTime.now());
        // when
        eventProducer.publish(event);

        // then
        // Create the consumer
        Map<String, Object> consumerProperties = getConsumerProperties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-single-0");

        try (KafkaConsumer<MyKeyAvro, MyEventAvro> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList("topic"));
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                ConsumerRecords<MyKeyAvro, MyEventAvro> records =
                                        consumer.poll(Duration.ofMillis(100));

                                if (records.isEmpty()) {
                                    return false;
                                }

                                return StreamSupport.stream(records.spliterator(), false)
                                        .filter(rec -> rec.key().getId().equals(event.id()))
                                        .anyMatch(
                                                rec ->
                                                        rec.value().getId().equals(event.id())
                                                        && event.version().equals(rec.value().getVersion())
                                                        && rec.value()
                                                                .getOccurredAt()
                                                                .equals(event.occurredAt().toString()));
                            });
        }
    }

    private static Map<String, Object> getConsumerProperties() {
        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        consumerProperties.put("key.deserializer", KafkaAvroDeserializer.class.getName());
        consumerProperties.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        consumerProperties.put("enable.auto.commit", "true");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("schema.registry.url", getSchemaRegistryUrl());
        consumerProperties.put("specific.avro.reader", true);
        return consumerProperties;
    }
}