package com.katariasoft.technologies.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NativeKafkaProducer {

	private static final String TOPIC = "new_test_topic";
	private static final String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";

	public NativeKafkaProducer() {
	}

	public static void main(String args[]) throws InterruptedException {

		// kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic new_test_topic
		// Creating propereties for kafka Producer.
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// creating Kafka producer.
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		// sending data to a topic .
		for (int i = 0; i < 1000; i++) {
			Thread.sleep(2000);
			kafkaProducer.send(new ProducerRecord<String, String>(TOPIC,  "From Java Native Producer Message: " + i));
		}

		// closing and flushing out producer .
		kafkaProducer.close();

	}

}
