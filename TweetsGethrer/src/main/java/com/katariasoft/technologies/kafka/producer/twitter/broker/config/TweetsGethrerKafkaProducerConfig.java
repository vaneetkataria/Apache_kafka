package com.katariasoft.technologies.kafka.producer.twitter.broker.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class TweetsGethrerKafkaProducerConfig {

	// ####Values to be defined###############
	// bootstrap.servers = [127.0.0.1:9092]
	// key.serializer = class org.apache.kafka.common.serialization.StringSerializer
	// value.serializer = class
	// org.apache.kafka.common.serialization.StringSerializer
	// max.block.ms = 60000
	// buffer.memory = 33554432
	// send.buffer.bytes = 131072
	// batch.size = 16384
	// linger.ms = 0
	// compression.type = none
	// enable.idempotence = false
	// max.request.size = 1048576
	// retries = 0
	// retry.backoff.ms = 100
	// max.in.flight.requests.per.connection = 5
	// acks = all
	// connections.max.idle.ms = 540000
	// reconnect.backoff.max.ms = 1000
	// reconnect.backoff.ms = 50
	// request.timeout.ms = 30000

	// ############# Values not studied and not configured
	// receive.buffer.bytes = 32768
	// client.id =
	// interceptor.classes = []
	// metadata.max.age.ms = 300000
	// metric.reporters = []
	// metrics.num.samples = 2
	// metrics.recording.level = INFO
	// metrics.sample.window.ms = 30000
	// partitioner.class = class
	// org.apache.kafka.clients.producer.internals.DefaultPartitioner
	// sasl.client.callback.handler.class = null
	// sasl.jaas.config = null
	// sasl.kerberos.kinit.cmd = /usr/bin/kinit
	// sasl.kerberos.min.time.before.relogin = 60000
	// sasl.kerberos.service.name = null
	// sasl.kerberos.ticket.renew.jitter = 0.05
	// sasl.kerberos.ticket.renew.window.factor = 0.8
	// sasl.login.callback.handler.class = null
	// sasl.login.class = null
	// sasl.login.refresh.buffer.seconds = 300
	// sasl.login.refresh.min.period.seconds = 60
	// sasl.login.refresh.window.factor = 0.8
	// sasl.login.refresh.window.jitter = 0.05
	// sasl.mechanism = GSSAPI
	// security.protocol = PLAINTEXT
	// ssl.cipher.suites = null
	// ssl.enabled.protocols = [TLSv1.2, TLSv1.1, TLSv1]
	// ssl.endpoint.identification.algorithm = https
	// ssl.key.password = null
	// ssl.keymanager.algorithm = SunX509
	// ssl.keystore.location = null
	// ssl.keystore.password = null
	// ssl.keystore.type = JKS
	// ssl.protocol = TLS
	// ssl.provider = null
	// ssl.secure.random.implementation = null
	// ssl.trustmanager.algorithm = PKIX
	// ssl.truststore.location = null
	// ssl.truststore.password = null
	// ssl.truststore.type = JKS
	// transaction.timeout.ms = 60000
	// transactional.id = null

	private static String BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092";

	public static Properties get() {

		Properties kafkaConfigs = new Properties();
		kafkaConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
		kafkaConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// increased max block ms to 80000 ms form 60000 ms to make more hope of putting
		// data into producer buffer.
		kafkaConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 80000);

		// ##
		// How many retries should a producer do . For high consistency and durability
		// set it to Max integer value.
		kafkaConfigs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
		// ##
		// acks can have value in
		// 1. [0(No acknowledgement of requesr received from cluster][High throghput and
		// low latency less durability.]
		// 2. [1(acknowledgement of data received by leader )][Medium throghput and
		// medium latency some what less durability.]
		// 3. [all(-1)(Acknowledgemnent of data received by leader and replicated to all
		// insync replicas)][low throghput and
		// high latency highest durability.]

		// Set min.insync.replicas =2 for high durability .
		// Have set in server.properties in kafka
		kafkaConfigs.put(ProducerConfig.ACKS_CONFIG, "all"); // can be [-1, 0 , 1 ] , -1 means all .
		// ##
		// In Kafka > 1.1 we can set MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5 and still
		// guarantee ordering
		kafkaConfigs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
		// ##
		// Enabling idempotence gurantess retries = max int value , acks = all and
		// MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION =5 and also stop de duplication of
		// kafka message commit in case if producer could not receive ack for the first
		// time.
		kafkaConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		// ##
		// compression can be snappy lz4 or gzip .
		kafkaConfigs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		// ##
		// Wait for 20 ms at least to send data to kafka if 32 kb batch is not filled.
		kafkaConfigs.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		// ##
		// Has increased batch size to 32 kb . If 32 kb is filled before 20 ms then
		// compression will be really good for this size. To increase high throughput
		// batch size has been increased.
		kafkaConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);

		return kafkaConfigs;

	}

}
