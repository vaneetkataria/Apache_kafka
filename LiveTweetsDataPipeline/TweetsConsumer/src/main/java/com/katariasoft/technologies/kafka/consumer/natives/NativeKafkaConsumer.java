package com.katariasoft.technologies.kafka.consumer.natives;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.katariasoft.technologies.kafka.consumer.util.Assert;

public class NativeKafkaConsumer<K, V> {
	private KafkaConsumer<K, V> kafkaConsumer;

	public NativeKafkaConsumer(Properties kafkaConsumerConfigs, String topic) {
		Assert.NotBlank(kafkaConsumerConfigs, "Kafka Consumer configs cannot be null or empty.");
		Assert.NotBlank(topic, "Kafka topic cannot be null or empty.");
		kafkaConsumer = new KafkaConsumer<>(kafkaConsumerConfigs);
		kafkaConsumer.subscribe(Arrays.asList(topic));
	}

	public ConsumerRecords<K, V> poll(long milliSeconds) {
		return kafkaConsumer.poll(Duration.ofMillis(milliSeconds));

	}

	public void stop() {
		kafkaConsumer.close();
	}

	public void commitSync() {
		kafkaConsumer.commitSync();
		;
	}

}
