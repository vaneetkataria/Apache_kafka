package com.katariasoft.technologies.kafka.producer.twitter.producer.helper;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.katariasoft.technologies.kafka.producer.exception.KafkaProducerException;
import com.katariasoft.technologies.kafka.producer.natives.CallbackNativeKafkaProducer;
import com.katariasoft.technologies.kafka.producer.twitter.stream.client.LiveTweetsStreamClient;
import com.katariasoft.technologies.kafka.producer.util.Assert;

public class LiveTweetsStreamKafkaForwarder implements StreamForwarder {

	private LiveTweetsStreamClient liveTweetsStreamClient;
	private CallbackNativeKafkaProducer<String, String> kafkaProducer;

	public LiveTweetsStreamKafkaForwarder(Properties kafkaConfigs, String kafkaTopic, List<String> tweetTerms) {
		try {
			liveTweetsStreamClient = new LiveTweetsStreamClient(tweetTerms);
			kafkaProducer = new CallbackNativeKafkaProducer<>(kafkaConfigs, kafkaTopic);
		} catch (Exception e) {
			System.err.println("Exception occured while initiating LiveTwitterStreamToKafkaForwarder .");
			stop();
			throw KafkaProducerException
					.instance("LiveTwitterStreamToKafkaForwarder could not be created because of exception . ", e);
		}
	}

	@Override
	public void forward() {
		Assert.nonNull("LiveTwitterStreamToKafkaForwarder not functionable as not initialised properly.",
				liveTweetsStreamClient, kafkaProducer);
		forwardLiveTwitterStreamToKafka();
	}

	public void stop() {
		if (Objects.nonNull(liveTweetsStreamClient))
			liveTweetsStreamClient.closeClient();
		if (Objects.nonNull(kafkaProducer))
			kafkaProducer.close();
	}

	public void forwardLiveTwitterStreamToKafka() {
		while (!liveTweetsStreamClient.isDone()) {
			processSingleDataSnapshot();
		}
	}

	private void processSingleDataSnapshot() {
		try {
			String message = liveTweetsStreamClient.getstreamingDataQueue().poll(2, TimeUnit.MILLISECONDS);
			if (Objects.nonNull(message)) {
				kafkaProducer.send(message);
				//System.out.println("Record delivered to kafka producer at :" + System.currentTimeMillis());
			}
		} catch (Exception e) {
			System.err
					.println("Exception occured while polling live data from queue. Processing next batch of records");
			e.printStackTrace();
		}
	}

}
