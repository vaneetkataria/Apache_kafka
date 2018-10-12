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
			System.out.println("Exception occured while initiating LiveTwitterStreamToKafkaForwarder .");
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
			try {
				String message = liveTweetsStreamClient.getstreamingDataQueue().poll(5, TimeUnit.MILLISECONDS);
				/* if (message != null) */
				kafkaProducer.send(message);
			} catch (Exception e) {
				handleExceptionGenrecally(e);
			}

		}

	}

	private void handleExceptionGenrecally(Exception e) {
		System.out.println("Exception occured while polling live data from queue. Stopping stream forwarding.");
		stop();
		throw KafkaProducerException
				.instance("Exception occured while polling live data from queue. Stopping stream forwarding.", e);
	}
}
