package com.katariasoft.technologies.kafka.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class NativeKafkaConsumerBackgroundRunnable implements Runnable {

	private KafkaConsumer<String, String> kafkaConsumer;
	private CountDownLatch latch;

	@Override
	public void run() {
		this.pollData();
	}

	public NativeKafkaConsumerBackgroundRunnable(Properties consumerConfigs, List<String> topics,
			CountDownLatch latch) {
		// validations
		if (consumerConfigs == null || consumerConfigs.isEmpty())
			throw new RuntimeException("Consumer consfiguration cannot be empty.");
		if (topics == null || topics.isEmpty())
			throw new RuntimeException("Topics to subscribe cannot be empty.");
		// creating consumer .
		kafkaConsumer = new KafkaConsumer<>(consumerConfigs);
		// subscribing consumer to topics
		kafkaConsumer.subscribe(topics);
		// latch creation.
		this.latch = latch;
	}

	public void pollData() {

		try {
			// consumer will poll data infinitely until poll background thread don't receive
			// an interrupt
			while (true) {
				// records consumed .
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : consumerRecords) {
					System.out.println("topic : " + record.topic() + "\n");
					System.out.println("partition : " + record.partition() + "\n");
					System.out.println("Key : " + record.key() + "\n");
					System.out.println("Value : " + record.value() + "\n");
					System.out.println("timestamp : " + record.timestamp() + "\n");
					System.out.println("++++++++");

				}

			}
		} catch (WakeupException ex) {
			System.out.println("Polling kafka Thread got inturupted with exception . Going to stop kafka consumer . ");
		} finally {
			kafkaConsumer.close();
			latch.countDown();
		}

	}

	public void shutDown() {
		System.out.println("Shutdown called in running background native kafka consumer thread.");
		System.out.println("Going to call wakeup on this kafka consumer polling records from kafka.");
		kafkaConsumer.wakeup();
	}

}
