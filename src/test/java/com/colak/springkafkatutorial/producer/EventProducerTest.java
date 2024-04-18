package com.colak.springkafkatutorial.producer;

import com.colak.springkafkatutorial.model.MyEvent;
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

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@Testcontainers
class EventProducerTest {

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

    @Autowired
    private PublisherListener publisherListener;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA_CONTAINER::getBootstrapServers);
        registry.add("spring.kafka.properties.schema.registry.url", EventProducerTest::getSchemaRegistryUrl);
    }

    private static String getSchemaRegistryUrl() {
        return String.format(
                "http://%s:%s", REGISTRY_CONTAINER.getHost(), REGISTRY_CONTAINER.getMappedPort(8081));
    }

    @Test
    void testWithListener() {
        // given
        MyEvent event = new MyEvent(UUID.randomUUID(), 1, LocalDateTime.now());

        // when
        eventProducer.publish(event);

        // then
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(
                        () -> this.publisherListener.getEventsSent().entrySet().stream()
                                .filter(entry -> entry.getKey().getId().equals(event.id()))
                                .map(Map.Entry::getValue)
                                .flatMap(java.util.List::stream)
                                .anyMatch(
                                        eventSent -> event.version().equals(eventSent.getVersion())));
    }
}