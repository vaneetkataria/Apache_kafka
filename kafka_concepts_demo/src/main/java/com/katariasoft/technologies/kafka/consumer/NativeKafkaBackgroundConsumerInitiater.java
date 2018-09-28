package com.katariasoft.technologies.kafka.consumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeKafkaBackgroundConsumerInitiater {

	private static final String TOPIC = "new_test_topic";
	private static final String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";
	private static final String GROUP_ID_CONFIG = "javacode";
	private static final String FROM_BEGINNING = "earliest";
	private static final Logger logger = LoggerFactory.getLogger(NativeKafkaBackgroundConsumerInitiater.class);

	public void init() {
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

		// creating a countdoun latch for wating for consumer thread to exit.
		CountDownLatch countDownLatch = new CountDownLatch(1);

		// creating a native kafka background consumer thread.
		NativeKafkaConsumerBackgroundRunnable myBackgroundConsumerRunnable = new NativeKafkaConsumerBackgroundRunnable(
				consumerConfigs, Collections.singletonList(TOPIC), countDownLatch);

		new Thread(myBackgroundConsumerRunnable).start();
		addShutDownHook(myBackgroundConsumerRunnable);
		waitForConsumerShutDown(countDownLatch);
	}

	public static void main(String args[]) {
		NativeKafkaBackgroundConsumerInitiater initiater = new NativeKafkaBackgroundConsumerInitiater();
		initiater.init();
	}

	private void addShutDownHook(final NativeKafkaConsumerBackgroundRunnable myBackgroundConsumerRunnable) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				System.out.println("Main Application geting shutdown . Stopping kafka consumer thread.");
				myBackgroundConsumerRunnable.shutDown();
			}
		}));

	}

	private void waitForConsumerShutDown(CountDownLatch countDownLatch) {
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			System.out.println("Main application exited");
		}
	}

}
