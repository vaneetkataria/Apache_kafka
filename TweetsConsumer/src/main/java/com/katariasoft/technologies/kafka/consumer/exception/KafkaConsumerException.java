package com.katariasoft.technologies.kafka.consumer.exception;

import java.util.Objects;

public class KafkaConsumerException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private KafkaConsumerException() {
		super();
	}

	private KafkaConsumerException(String message) {
		super(message);
	}

	private KafkaConsumerException(String message, Throwable throwable) {
		super(message, throwable);
	}

	private KafkaConsumerException(Throwable throwable) {
		super(throwable);
	}

	public static KafkaConsumerException instance(String message, Throwable throwable) {
		return (Objects.nonNull(message) && Objects.nonNull(throwable)) ? new KafkaConsumerException(message, throwable)
				: (Objects.nonNull(message)) ? new KafkaConsumerException(message)
						: (Objects.nonNull(throwable)) ? new KafkaConsumerException(throwable)
								: new KafkaConsumerException();
	}

}
