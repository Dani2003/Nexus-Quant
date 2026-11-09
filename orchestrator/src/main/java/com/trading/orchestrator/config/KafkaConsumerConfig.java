package com.trading.orchestrator.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Spring Kafka configuration wired with Java 21 Project Loom Virtual Threads
 * and manual acknowledgment tied to full processing completion.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:trading-orchestrator-group}")
    private String groupId;

    @Value("${trading.kafka.concurrency:4}")
    private int concurrency;

    /**
     * Dedicated Java 21 Virtual Thread Task Executor for Kafka listener dispatchers.
     */
    @Bean
    public AsyncTaskExecutor kafkaVirtualThreadExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("kafka-vt-");
        executor.setVirtualThreads(true);
        return executor;
    }

    /**
     * Virtual thread pool executor service for internal async pipeline processing.
     */
    @Bean(destroyMethod = "close")
    public ExecutorService virtualThreadPerTaskExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Disabled for explicit manual ack
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Error handler with 3 retry attempts and exponential backoff.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // Retry 3 times with 200ms delay between attempts before logging error
        return new DefaultErrorHandler(new FixedBackOff(200L, 3L));
    }

    /**
     * ConcurrentKafkaListenerContainerFactory configured with Virtual Threads
     * and MANUAL_IMMEDIATE ack mode to tie Kafka commits strictly to processing completion.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            AsyncTaskExecutor kafkaVirtualThreadExecutor,
            DefaultErrorHandler kafkaErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);

        // Assign the virtual thread executor
        factory.getContainerProperties().setListenerTaskExecutor(kafkaVirtualThreadExecutor);

        // MANUAL_IMMEDIATE ensures offsets are committed only after processing succeeds
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }
}
