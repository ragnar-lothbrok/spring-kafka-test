package com.springkafka.api;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumedEventCache {

	public ConsumerRecord<String, String> get();

	public Boolean add(ConsumerRecord<String, String> event);

	ConsumerRecord<String, String> get(Integer time, TimeUnit timeUnit);
}
