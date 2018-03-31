package com.springkafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.MessageListener;

import com.springkafka.api.ConsumedEventCache;

public class EventMessageListener implements MessageListener<String, String> {

	private static final Logger logger = LoggerFactory.getLogger(EventMessageListener.class);

	private ConsumedEventCache consumedEventCache;

	public EventMessageListener(ConsumedEventCache consumedEventCache) {
		this.consumedEventCache = consumedEventCache;
	}

	@Override
	public void onMessage(ConsumerRecord<String, String> data) {
		try {
			consumedEventCache.add(data);
		} catch (Exception e) {
			logger.error("exception occured while adding event to queue = {} ", e);
		}
	}

}
