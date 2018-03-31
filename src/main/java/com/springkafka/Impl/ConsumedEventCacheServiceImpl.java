package com.springkafka.Impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.springkafka.api.ConsumedEventCache;

@Service
public class ConsumedEventCacheServiceImpl implements ConsumedEventCache {

	private static final Logger logger = LoggerFactory.getLogger(ConsumedEventCacheServiceImpl.class);

	private BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

	@Override
	public ConsumerRecord<String, String> get(Integer time, TimeUnit timeUnit) {
		try {
			return records.poll(time, timeUnit);
		} catch (InterruptedException e) {
			logger.error("Exception occured while getting top element from queue = {} ", e);
		}
		return null;
	}

	@Override
	public ConsumerRecord<String, String> get() {
		return records.poll();
	}

	@Override
	public Boolean add(ConsumerRecord<String, String> event) {
		return records.add(event);
	}

}
