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
		consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
		// Strategy for offset consideration while starting consumers .
		// earliest/latest/none
		// earliest means --from-beginning
		// latest means from the latest offset.
		// none means throw exception is no offset is found .
		// --from-beginning
		consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		// Setting it false means code will manually commit the offsets .
		// By default this setting is true
		// After every 5 seconds offsets are committed .
		// With this approach data is processed first then offsets are committed
		// manually . Even while processing if some consumer goes down next time same
		// offsets will be picked up . If consumer is idempotent then there will be no
		// issues in processing this data again .
		// In default approach every 5 seconds offsets will be committed which can
		// create a stage where some time offsets for unprocessed records are committed
		// and if while
		// processing such
		// records if some problem occurs (This consumer goes down) then this data will
		// be lost and will not be able to read from other consumers as wrong offsets
		// have been committed .
		// Manual commit is always preferred.
		consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		// At least 2048 bytes should be present on server to return the response other
		// wise consumer will wait .
		// This will improve throughput at cost of some latency.
		consumerConfigs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 2048);
		// this much milli seconds server will wait to send empty data if
		// FETCH_MIN_BYTES_CONFIG requirement is not fulfilled .
		consumerConfigs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 2000);
		// Max 2 MB will be returned from single partition in one poll.
		consumerConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10 * 1024 * 1024);
		// 50 MB default value .Maximum 50 MB of data will only be returned in one poll
		// request.
		consumerConfigs.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 70 * 1024 * 1024);

		// Max records to be returned in a single poll call .
		consumerConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		// All consumer present in consumer group send heart beat to consumer
		// coordinator (In broker nodes )for their liveliness every
		// HEARTBEAT_INTERVAL_MS_CONFIG milliseconds . This second should be 1/3 of
		// SESSION_TIMEOUT_MS_CONFIG as a guideline.
		consumerConfigs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5);
		// If no heart beat is received from any consumer in group till 15 ms then re
		// balance will be initiated .
		consumerConfigs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15);
		// If consumer didn't put any poll request till 10 sec rebalnce will happen .
		consumerConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10 * 60 * 1000);

		return consumerConfigs;

	}

}
