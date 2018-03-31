package com.springkafka.api;

import java.util.Map;

public interface EventProducer {

	void sendEvent(String topic, Map<String, String> header, String body);
}
