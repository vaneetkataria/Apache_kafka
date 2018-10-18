package com.katariasoft.technologies.kafka.producer.natives;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.katariasoft.technologies.kafka.producer.util.Assert;

public class CallbackNativeKafkaProducer<K, V> {

	private Properties configs;
	private String topic;
	private KafkaProducer<K, V> kafkaProducer;

	public CallbackNativeKafkaProducer() {
	}

	public CallbackNativeKafkaProducer(Properties configs, String topic) {
		Assert.NotBlank(configs, "Cannot create LiveTwitterStreamToKafkaForwarder . Kafka Configs must be defined. ");
		Assert.NotBlank(topic, "Cannot create LiveTwitterStreamToKafkaForwarder . Kafka Topic must be defined. ");
		this.configs = configs;
		this.topic = topic;
		kafkaProducer = new KafkaProducer<>(configs);
	}

	public void send(V value) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, value);
		kafkaProducer.send(producerRecord, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (Objects.nonNull(exception))
					exception.printStackTrace();
				else {
					System.out.println("Topic:" + metadata.topic() + "\n");
					System.out.println("Partition:" + metadata.partition() + "\n");
					System.out.println("Offset:" + metadata.offset() + "\n");
					System.out.println("TimeStamp:" + metadata.timestamp() + "\n");
					System.out.println("+++++");
				}
			}
		});
	}

	public void close() {
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
