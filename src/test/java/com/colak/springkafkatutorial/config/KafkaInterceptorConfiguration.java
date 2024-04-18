package com.colak.springkafkatutorial.config;

import com.colak.kafkapublishertest.avro.MyEventAvro;
import com.colak.kafkapublishertest.avro.MyKeyAvro;
import com.colak.springkafkatutorial.producer.PublisherListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaInterceptorConfiguration {

    @Bean
    PublisherListener publisherInterceptor() {
        return new PublisherListener();
    }

    // Customize the KafkaTemplate to add the PublisherInterceptor
    // Specific configuration for testing
    @Bean
    public KafkaTemplate<MyKeyAvro, MyEventAvro> kafkaTemplate(
            ProducerFactory<MyKeyAvro, MyEventAvro> producerFactory,
            PublisherListener publisherListener) {
        KafkaTemplate<MyKeyAvro, MyEventAvro> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setProducerListener(publisherListener);
        return kafkaTemplate;
    }
}
