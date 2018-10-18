package com.katariasoft.technologies.kafka.consumer.twitter.consumer.helper;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.katariasoft.technologies.kafka.consumer.exception.KafkaConsumerException;
import com.katariasoft.technologies.kafka.consumer.natives.NativeKafkaConsumer;
import com.katariasoft.technologies.kafka.consumer.twitter.elasticsearch.client.ElasticSerachClientProvider;
import com.katariasoft.technologies.kafka.consumer.util.Assert;

/**
 * @author YATRAONLINE\vaneet.kumar
 *
 * @param <K>
 * @param <V>
 */
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
			if (Objects.nonNull(consumerRecords) && !consumerRecords.isEmpty()) {
				BulkRequest bulkRequest = new BulkRequest();
				for (ConsumerRecord<K, V> record : consumerRecords) {
					logConsumedData(record);
					if (Objects.nonNull(record)) {
						addToBulkRequest(record, bulkRequest);
					}
				}
				if (forwardToElasticSearch(bulkRequest))
					nativeKafkaConsumer.commitSync();
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

	private void addToBulkRequest(ConsumerRecord<K, V> record, BulkRequest bulkRequest) {
		IndexRequest request = new IndexRequest(esIndex, esIndexType);
		// To make consumer idempotent . As after processing completed data and just
		// before committing offset if consumer died and when new consumer will be
		// poll this data again (As offsets were not committed by old consumer )then
		// duplication of data should not happen in elastic
		// search.
		request.id(record.topic() + "_" + record.partition() + "_" + record.offset());
		 request.source(record.value(), XContentType.JSON);
		// This JSON string is for testing purpose only.
		//request.source("{\"a\":\"b\"}", XContentType.JSON);

		bulkRequest.add(request);
	}

	private boolean forwardToElasticSearch(BulkRequest bulkRequest) {
		try {
			BulkResponse response = elasticSerachClient.bulk(bulkRequest, RequestOptions.DEFAULT);
			return true;
		} catch (IOException e) {
			System.err.println("IO Exception occured while creating index in elastic search cluster.");
			e.printStackTrace();
			return false;
		}
	}

	private void logConsumedData(ConsumerRecord<K, V> record) {
		System.out.println("topic : " + record.topic() + "\n");
		System.out.println("partition : " + record.partition() + "\n");
		System.out.println("offset: " + record.offset() + "\n");
		System.out.println("Key : " + record.key() + "\n");
		System.out.println("Value : " + record.value() + "\n");
		System.out.println("timestamp : " + record.timestamp() + "\n");
		System.out.println("++++++++");
	}

}
