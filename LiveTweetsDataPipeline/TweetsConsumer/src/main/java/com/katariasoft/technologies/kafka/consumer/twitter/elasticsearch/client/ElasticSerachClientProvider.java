package com.katariasoft.technologies.kafka.consumer.twitter.elasticsearch.client;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSerachClientProvider {

	private static ElasticSearchClientConfiguration clientConfig = new ElasticSearchClientConfiguration();

	public static RestHighLevelClient provide() {
		return ElasticSerachClientIntanceHolder.getInstance();
	}

	private static class ElasticSerachClientIntanceHolder {
		private static RestHighLevelClient restHighLevelClient;

		static {
			try {
				final CredentialsProvider provider = new BasicCredentialsProvider();
				provider.setCredentials(AuthScope.ANY,
						new UsernamePasswordCredentials(clientConfig.getUserName(), clientConfig.getPassword()));

				RestClientBuilder builder = RestClient.builder(
						new HttpHost(clientConfig.getHostName(), clientConfig.getPort(), clientConfig.getScheme()))
						.setHttpClientConfigCallback(new HttpClientConfigCallback() {

							@Override
							public HttpAsyncClientBuilder customizeHttpClient(
									HttpAsyncClientBuilder httpClientBuilder) {
								// TODO Auto-generated method stub
								return httpClientBuilder.setDefaultCredentialsProvider(provider);
							}
						});

				restHighLevelClient = new RestHighLevelClient(builder);
			} catch (Exception e) {
				System.err.println(
						"Exception occured while creating RestHighLevelClient for elastic search . Throwing exception.");
				throw e;
			}

		}

		private static RestHighLevelClient getInstance() {
			return restHighLevelClient;
		}

	}
}
