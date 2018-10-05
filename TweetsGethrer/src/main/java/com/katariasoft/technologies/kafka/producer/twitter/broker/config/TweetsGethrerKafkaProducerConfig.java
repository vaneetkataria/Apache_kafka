package com.katariasoft.technologies.kafka.producer.twitter.broker.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class TweetsGethrerKafkaProducerConfig {

	private static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";

	public static Properties get() {

		Properties kafkaConfigs = new Properties();
		kafkaConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		kafkaConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaConfigs.put(ProducerConfig.ACKS_CONFIG, "all"); // can be [-1, 0 , 1 ] , -1 means all .

		return kafkaConfigs;

	}

}
