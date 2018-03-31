package com.springkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.springkafka.api.ConsumedEventCache;
import com.springkafka.api.EventProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class SpringKafkaSenderTest {

	@Autowired
	private EventProducer eventProducer;

	@Autowired
	private ConsumedEventCache consumedEventCache;

	private static final String TOPIC = "test_one";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TOPIC);

	public Map<String, String> getHeader() {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("ID", UUID.randomUUID().toString());
		return headers;
	}

	public String getBody() {
		return "TEST DATA";
	}

	@Test
	public void testSend() throws InterruptedException {
		eventProducer.sendEvent(TOPIC, getHeader(), getBody());

		ConsumerRecord<String, String> received = consumedEventCache.get(100, TimeUnit.SECONDS);

		assertThat(received, hasValue(getBody()));

	}

}