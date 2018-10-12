package com.katariasoft.technologies.kafka.consumer.twitter.consumer.helper;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.katariasoft.technologies.kafka.consumer.exception.KafkaConsumerException;
import com.katariasoft.technologies.kafka.consumer.natives.NativeKafkaConsumer;
import com.katariasoft.technologies.kafka.consumer.twitter.elasticsearch.client.ElasticSerachClientProvider;
import com.katariasoft.technologies.kafka.consumer.util.Assert;

public class LiveTweetsStreamKafkaToElasticSearchForwarder<K, V> implements StreamForwarder {

	private NativeKafkaConsumer<K, V> nativeKafkaConsumer;
	private RestHighLevelClient elasticSerachClient;
	private String esIndex;
	private String esIndexType;

	private static final String CONFIGURTION_ERROR = "LiveTweetsStreamKafkaToElasticSearchForwarder instance not configured properly.";

	public LiveTweetsStreamKafkaToElasticSearchForwarder(Properties kafkaConfigs, String topic, String esIndex,
			String esIndexType) {
		try {
			nativeKafkaConsumer = new NativeKafkaConsumer<>(kafkaConfigs, topic);
			elasticSerachClient = ElasticSerachClientProvider.provide();
			this.esIndex = esIndex;
			this.esIndexType = esIndexType;
		} catch (Exception e) {
			stop();
			KafkaConsumerException
					.instance("Exception occured while creating LiveTweetsStreamKafkaToElasticSearchForwarder .", e);

		}
	}

	@Override
	public void forward() {
		Assert.nonNull(CONFIGURTION_ERROR, nativeKafkaConsumer, elasticSerachClient);
		Assert.NotBlank(esIndex, CONFIGURTION_ERROR);
		Assert.NotBlank(esIndex, CONFIGURTION_ERROR);
		while (true) {
			processSingleDataBatch();
		}
	}

	private void processSingleDataBatch() {
		try {
			ConsumerRecords<K, V> consumerRecords = nativeKafkaConsumer.poll(100);
			for (ConsumerRecord<K, V> record : consumerRecords) {
				logConsumedData(record);
				if (Objects.nonNull(record)) {
					forwardToElasticSearch(record);
				}
			}
		} catch (WakeupException | InterruptException e) {
			System.err.println(
					"This caller Thread was interuptted or wakeup method was called on KafkaConsumer before or while calling poll method. Continuing processing for next batch.  ");
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println(
					"Exception occured while stream forwarding from kafka to elastic search . Rethrowing exception Processing stopped.");
			throw KafkaConsumerException.instance(
					"Exception occured while stream forwarding from kafka to elastic search . Rethrowing exception Processing stopped.",
					e);
		}
	}

	@Override
	public void stop() {
		try {
			if (Objects.nonNull(nativeKafkaConsumer))
				nativeKafkaConsumer.stop();
			if (Objects.nonNull(elasticSerachClient))
				elasticSerachClient.close();
		} catch (Exception e) {
			throw KafkaConsumerException
					.instance("Exception occured while stopping LiveTweetsStreamKafkaToElasticSearchForwarder .", e);
		}
	}

	private void forwardToElasticSearch(ConsumerRecord<K, V> record) {
		try {
			IndexRequest request = new IndexRequest(esIndex, esIndexType);
			request.source(record.value(), XContentType.JSON);
			elasticSerachClient.index(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			System.err.println("IO Exception occured while creating index in elastic search cluster.");
			e.printStackTrace();
		}
	}

	private void logConsumedData(ConsumerRecord<K, V> record) {
		System.out.println("topic : " + record.topic() + "\n");
		System.out.println("partition : " + record.partition() + "\n");
		System.out.println("Key : " + record.key() + "\n");
		System.out.println("Value : " + record.value() + "\n");
		System.out.println("timestamp : " + record.timestamp() + "\n");
		System.out.println("++++++++");
	}

}
