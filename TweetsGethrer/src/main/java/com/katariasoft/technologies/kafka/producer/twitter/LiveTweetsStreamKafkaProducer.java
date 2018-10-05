package com.katariasoft.technologies.kafka.producer.twitter;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.katariasoft.technologies.kafka.producer.twitter.broker.config.TweetsGethrerKafkaProducerConfig;

public class LiveTweetsStreamKafkaProducer {

	private static String TOPIC = "twitterstream";
	private static Properties kafkaConfigs;
	private static List<String> tweetTerms;

	static {
		// kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic new_test_topic
		// Creating properties for kafka Producer.
		// interested tweet terms
		tweetTerms = Arrays.asList("avengers");
		// kafka configs
		kafkaConfigs = TweetsGethrerKafkaProducerConfig.get();

	}

	private LiveTweetsStreamKafkaForwarder streamForwarder;

	public LiveTweetsStreamKafkaProducer(Properties kafkaConfigs, String kafkaTopic, List<String> tweetTerms) {
		try {
			streamForwarder = new LiveTweetsStreamKafkaForwarder(kafkaConfigs, kafkaTopic, tweetTerms);
		} catch (Exception e) {
			System.out.println(
					"Exception occured while creating  LiveTwitterStreamKafkaProducer throwing exception with cause.");
			throw new RuntimeException(
					"Exception occured while creating  LiveTwitterStreamKafkaProducer throwing exception with cause.",
					e);
		}
	}

	public void produce() {
		streamForwarder.forward();
	}

	public static void main(String args[]) {
		try {
			new LiveTweetsStreamKafkaProducer(kafkaConfigs, TOPIC, tweetTerms).produce();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
