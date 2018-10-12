package com.katariasoft.technologies.kafka.producer.exception;

import java.util.Objects;

public class KafkaProducerException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private KafkaProducerException() {
		super();
	}

	private KafkaProducerException(String message) {
		super(message);
	}

	private KafkaProducerException(String message, Throwable throwable) {
		super(message, throwable);
	}

	private KafkaProducerException(Throwable throwable) {
		super(throwable);
	}

	public static KafkaProducerException instance(String message, Throwable throwable) {
		if (Objects.nonNull(message) && Objects.nonNull(throwable))
			return new KafkaProducerException(message, throwable);
		if (Objects.nonNull(message))
			return new KafkaProducerException(message);
		if (Objects.nonNull(throwable))
			return new KafkaProducerException(throwable);
		else
			return new KafkaProducerException();
	}

}
