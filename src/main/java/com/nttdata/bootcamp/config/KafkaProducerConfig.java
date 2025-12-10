package com.nttdata.bootcamp.config;

import com.nttdata.bootcamp.events.EventKafka;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaSender<String, EventKafka<?>> kafkaSender() {

        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Importante para evitar problemas de des/serialización
        props.put("spring.json.add.type.headers", false);
        props.put("spring.json.trusted.packages", "*");

        SenderOptions<String, EventKafka<?>> senderOptions = SenderOptions.create(props);

        return KafkaSender.create(senderOptions);
    }
}
