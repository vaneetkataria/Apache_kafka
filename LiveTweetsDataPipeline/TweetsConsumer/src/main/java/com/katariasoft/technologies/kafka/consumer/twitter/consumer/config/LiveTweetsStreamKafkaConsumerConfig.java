package com.katariasoft.technologies.kafka.consumer.twitter.consumer.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LiveTweetsStreamKafkaConsumerConfig {

	private static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
	private static String GROUP_ID_CONFIG = "javacode";

	public static Properties get() {

		// kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092
		// --group javaCode --topic new_test_topic --from-beginning

		// creating kafka consumer configs
		Properties consumerConfigs = new Properties();
		consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// --from-beginning
		consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
		return consumerConfigs;

	}

}
