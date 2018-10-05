package com.katariasoft.technologies.kafka.producer.natives;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class CallbackNativeKafkaProducer<K, V> {

	private Properties configs;
	private String topic;
	private KafkaProducer<K, V> kafkaProducer;

	public CallbackNativeKafkaProducer() {
	}

	public CallbackNativeKafkaProducer(Properties configs, String topic) {
		assertInitiatable(configs, topic);
		this.configs = configs;
		this.topic = topic;
		kafkaProducer = new KafkaProducer<>(configs);
	}

	public void send(K key, V value) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, value);
		kafkaProducer.send(producerRecord, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {

				if (exception != null)
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

	private void assertInitiatable(Properties kafkaConfigs, String kafkaTopic) {
		if (kafkaConfigs == null || kafkaConfigs.isEmpty() || kafkaTopic == null || kafkaTopic.isEmpty())
			throw new RuntimeException(
					"Cannot create LiveTwitterStreamToKafkaForwarder . Configs and topics must be defined. ");
	}

	// for testing purpose only
	public static void main(String args[]) throws InterruptedException {

		String TOPIC = "new_test_topic";
		String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";

		// kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic new_test_topic
		// Creating properties for kafka Producer.
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.ACKS_CONFIG, "all"); // can be [-1, 0 , 1 ] , -1 means all .

		// creating Kafka producer.
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		// sending data to a topic .
		for (int i = 0; i < 1000; i++) {
			Thread.sleep(2000);
			kafkaProducer.send(
					new ProducerRecord<String, String>(TOPIC, "From  Callback Java Native Producer Message: " + i),

					new Callback() {
						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {

							if (exception != null)
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

		// closing and flushing out producer .
		kafkaProducer.close();

	}

}
