package com.katariasoft.technologies.kafka.consumer.twitter.consumer;

import java.util.Properties;

import com.katariasoft.technologies.kafka.consumer.twitter.consumer.config.LiveTweetsStreamKafkaConsumerConfig;
import com.katariasoft.technologies.kafka.consumer.twitter.consumer.helper.LiveTweetsStreamKafkaToElasticSearchForwarder;
import com.katariasoft.technologies.kafka.consumer.util.Assert;

public class LiveTweetsKafkaToElasticSearchPipeline {

	private static final String TOPIC = "twitterstream";
	private static final String ES_INDEX = "medium";
	private static final String ES_INDEX_TYPE = "posts";

	private LiveTweetsStreamKafkaToElasticSearchForwarder forwarder;
	private Properties kafkaConfigs;
	private String topic;
	private String esIndex;
	private String esIndexType;

	public LiveTweetsKafkaToElasticSearchPipeline(Properties kafkaConfigs, String topic, String esIndex,
			String esIndexType) {
		Assert.NotBlank(kafkaConfigs, "Kafka Configuration msut be defined.");
		Assert.NotBlank(topic, "Kafka Topic must be defined .");
		Assert.NotBlank(esIndex, "Elastic search index must be defined.");
		Assert.NotBlank(esIndexType, "Elastic search index type must be defined.");
		this.kafkaConfigs = kafkaConfigs;
		this.topic = topic;
		this.esIndex = esIndex;
		this.esIndexType = esIndex;
		forwarder = new LiveTweetsStreamKafkaToElasticSearchForwarder(LiveTweetsStreamKafkaConsumerConfig.get(), topic,
				esIndex, esIndexType);
	}

	public void start() {
		forwarder.forward();
	}

	public static void main(String args[]) {
		try {
			LiveTweetsKafkaToElasticSearchPipeline pipeline = new LiveTweetsKafkaToElasticSearchPipeline(
					LiveTweetsStreamKafkaConsumerConfig.get(), TOPIC, ES_INDEX, ES_INDEX_TYPE);
			pipeline.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
