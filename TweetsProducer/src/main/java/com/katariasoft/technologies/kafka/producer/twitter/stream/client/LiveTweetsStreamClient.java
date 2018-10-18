package com.katariasoft.technologies.kafka.producer.twitter.stream.client;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class LiveTweetsStreamClient {

	// secure static variables which should come from externalised properties.
	private static final String consumerKey = "bxEgtETXvTBERJxqhIkUeko9t";
	private static final String consumerSecret = "YaPnbqC61jHGpAeSBDKyig2r0HEq39IN7niKnRQvr0EtgC8dTy";
	private static final String token = "607179133-AdBA9k6wsihxpX8210Xd0YJovSK8D8BaT0N37hsA";
	private static final String secret = "NL7chMgrKbiFCfUSZA5bHFvdtA9lFJWGIuaj1NHGsF19M";

	// It should have size as per memory and live data coming .
	private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
	// twitter client
	private Client hosebirdClient;

	public LiveTweetsStreamClient(List<String> tweetTerms) {
		try {
			Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
			StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
			hosebirdEndpoint.trackTerms(tweetTerms);

			ClientBuilder builder = new ClientBuilder().name("TweetsGetherer").hosts(hosebirdHosts)
					.authentication(new OAuth1(consumerKey, consumerSecret, token, secret)).endpoint(hosebirdEndpoint)
					.processor(new StringDelimitedProcessor(msgQueue));

			hosebirdClient = builder.build();
			hosebirdClient.connect();
		} catch (Exception e) {
			System.out.println("Could not create LiveTweetsStreamClient as some exception occured while creating it. ");
			throw e;
		}

	}

	public boolean isDone() {
		return hosebirdClient.isDone();
	}

	public BlockingQueue<String> getstreamingDataQueue() {
		return msgQueue;
	}

	public void closeClient() {
		hosebirdClient.stop();
	}

}
