package com.katariasoft.technologies.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AssignAndSeekKafkaConsumer {

	private static final String TOPIC = "new_test_topic";
	private static final String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
	private static final String GROUP_ID_CONFIG = "javacode";
	private static final String FROM_BEGINNING = "earliest";
	private static final String REAL_TIME_ONLY = "latest";

	public static void main(String args[]) {

		// kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092
		// --group javaCode --topic new_test_topic --from-beginning

		// creating kafka consumer configs
		Properties consumerConfigs = new Properties();
		consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// --from-beginning
		consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, FROM_BEGINNING);
		consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);

		// creating kafka consumer .
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);

		TopicPartition topicPartition = new TopicPartition(TOPIC, 1);

		// subscribing kafka consumer to a topic
		consumer.assign(Arrays.asList(topicPartition));
		consumer.seek(topicPartition, 15L);

		int numMessages = 5;
		int messagesRead = 0;

		while (messagesRead < numMessages) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord record : records) {

				System.out.println("topic : " + record.topic() + "\n");
				System.out.println("partition : " + record.partition() + "\n");
				System.out.println("Key : " + record.key() + "\n");
				System.out.println("Value : " + record.value() + "\n");
				System.out.println("timestamp : " + record.timestamp() + "\n");
				System.out.println("++++++++");

				messagesRead++;
				if (messagesRead >= numMessages)
					break;

			}
		}

	}

}
