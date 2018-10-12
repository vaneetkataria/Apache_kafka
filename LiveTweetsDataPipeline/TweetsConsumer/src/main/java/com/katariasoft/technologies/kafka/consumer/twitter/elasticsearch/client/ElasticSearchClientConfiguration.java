package com.katariasoft.technologies.kafka.consumer.twitter.elasticsearch.client;

/* A class to hold hard coded credentials and connection specific data of elastic search . 
This data should come from externalised properties only . 

*https://aa4x2qiqjl:mfnbtlp3rd@tweetsgethrer-8966050034.ap-southeast-2.bonsaisearch.net
*
*/
class ElasticSearchClientConfiguration {

	private String hostName = "tweetsgethrer-8966050034.ap-southeast-2.bonsaisearch.net";
	private String userName = "aa4x2qiqjl";
	private String password = "mfnbtlp3rd";
	private String scheme = "https";
	private int port = 443;

	String getHostName() {
		return hostName;
	}

	void setHostName(String hostName) {
		this.hostName = hostName;
	}

	String getUserName() {
		return userName;
	}

	void setUserName(String userName) {
		this.userName = userName;
	}

	String getPassword() {
		return password;
	}

	void setPassword(String password) {
		this.password = password;
	}

	String getScheme() {
		return scheme;
	}

	void setScheme(String scheme) {
		this.scheme = scheme;
	}

	int getPort() {
		return port;
	}

	void setPort(int port) {
		this.port = port;
	}

}
