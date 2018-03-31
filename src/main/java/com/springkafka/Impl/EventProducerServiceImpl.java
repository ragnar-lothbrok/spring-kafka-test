package com.springkafka.Impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.springkafka.api.EventProducer;
import com.springkafka.config.KafkaProperties;


@Service
public class EventProducerServiceImpl implements EventProducer {

	private static final Logger logger = LoggerFactory.getLogger(EventProducerServiceImpl.class);

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Override
	public void sendEvent(String topic, Map<String,String> headers, String body) {
		logger.info("header = {} body = {} ", headers, body);

		List<RecordHeader> kafkaHeaders = new ArrayList<RecordHeader>();
		if(headers != null && headers.size() > 0) {
			for(Entry<String,String> header : headers.entrySet()) {
				kafkaHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
			}
		}
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,
				(body.hashCode() % kafkaProperties.getPartitionCount()), body, body,
				new RecordHeaders(kafkaHeaders.toArray(new RecordHeader[kafkaHeaders.size()])));
		try {
			Future<SendResult<String, String>> futureObj = template.send(record);
			futureObj.get();
			logger.info("record sent.");
		} catch (Exception exception) {
			logger.error("Exception occured while sending = {} ", exception);
		}
	}

}
