package com.katariasoft.technologies.kafka.consumer.util;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class Assert {

	public static void proceedable(boolean expression, String message) {
		if (!expression)
			throw new RuntimeException(message);
	}

	public static void NotBlank(String value, String message) {
		if (Objects.isNull(value) || value.isEmpty())
			throw new IllegalArgumentException(message);
	}

	public static <T> void NotBlank(Collection<T> value, String message) {
		if (Objects.isNull(value) || value.isEmpty())
			throw new IllegalArgumentException(message);
	}

	public static <K, V> void NotBlank(Map<K, V> value, String message) {
		if (Objects.isNull(value) || value.isEmpty())
			throw new IllegalArgumentException(message);
	}

	public static void nonNull(String message, Object... objects) {
		if (Objects.isNull(objects) || objects.length == 0)
			throw new NullPointerException(message);
		for (Object o : objects)
			Objects.requireNonNull(o, message);
	}

}
