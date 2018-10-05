package com.katariasoft.technologies.kafka.producer.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.katariasoft.technologies.kafka.producer.natives.CallbackNativeKafkaProducer;

class LiveTweetsStreamKafkaForwarder implements StreamForwarder {

	private LiveTweetsStreamClient liveTweetsStreamClient;
	private CallbackNativeKafkaProducer<String, String> kafkaProducer;

	public LiveTweetsStreamKafkaForwarder(Properties kafkaConfigs, String kafkaTopic, List<String> tweetTerms) {
		try {
			liveTweetsStreamClient = new LiveTweetsStreamClient(tweetTerms);
			kafkaProducer = new CallbackNativeKafkaProducer<>(kafkaConfigs, kafkaTopic);
		} catch (Exception e) {
			System.out.println("Exception occured while initiating LiveTwitterStreamToKafkaForwarder .");
			stop();
			throw new RuntimeException("LiveTwitterStreamToKafkaForwarder could not be created because of exception . ",
					e);
		}
	}

	@Override
	public void forward() {
		assertFunctionalble();
		forwardLiveTwitterStreamToKafka();
	}

	public void stop() {
		if (liveTweetsStreamClient != null)
			liveTweetsStreamClient.closeClient();
		if (kafkaProducer != null)
			kafkaProducer.close();
	}

	public void forwardLiveTwitterStreamToKafka() {
		while (!liveTweetsStreamClient.isDone()) {
			try {
				String message = liveTweetsStreamClient.getstreamingDataQueue().poll(5, TimeUnit.MILLISECONDS);
				if (message != null)
					kafkaProducer.send(message);
			} catch (Exception e) {
				handleExceptionGenrecally(e);
			}

		}

	}

	private void handleExceptionGenrecally(Exception e) {
		System.out.println("Exception occured while polling live data from queue. Stopping stream forwarding.");
		stop();
		throw new RuntimeException("Exception occured while polling live data from queue. Stopping stream forwarding.",
				e);
	}

	private void assertFunctionalble() {
		if (liveTweetsStreamClient == null || kafkaProducer == null)
			throw new RuntimeException(
					"LiveTwitterStreamToKafkaForwarder not functionable as not initialised properly. ");

	}

}
