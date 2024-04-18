package com.colak.springkafkatutorial.producer;

import com.colak.kafkapublishertest.avro.MyEventAvro;
import com.colak.kafkapublishertest.avro.MyKeyAvro;
import com.colak.springkafkatutorial.model.MyEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<MyKeyAvro, MyEventAvro> kafkaTemplate;

    public void publish(MyEvent event) {
        // Map Event to avro
        MyEventAvro myEventAvro =
                MyEventAvro.newBuilder()
                        .setId(event.id())
                        .setVersion(event.version())
                        .setOccurredAt(event.occurredAt().toString())
                        .build();

        MyKeyAvro myKeyAvro = MyKeyAvro.newBuilder().setId(event.id()).build();

        this.kafkaTemplate.send("topic", myKeyAvro, myEventAvro);
    }
}
