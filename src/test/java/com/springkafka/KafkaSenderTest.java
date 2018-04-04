package com.springkafka;

import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import com.springkafka.api.ConsumedEventCache;
import com.springkafka.config.KafkaProperties;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
public class KafkaSenderTest {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Autowired
	private KafkaProperties kafkaProperties;

	@Autowired
	private ConsumedEventCache consumedEventCache;

	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.RETRIES_CONFIG, kafkaProperties.getRetries());
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
				"org.apache.kafka.clients.producer.internals.DefaultPartitioner");
		return props;
	}

	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

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

		KafkaTemplate<String, String> kafkaTemplate = kafkaTemplate();

		List<RecordHeader> kafkaHeaders = new ArrayList<RecordHeader>();
		if (getHeader() != null && getHeader().size() > 0) {
			for (Entry<String, String> header : getHeader().entrySet()) {
				kafkaHeaders.add(new RecordHeader(header.getKey(), header.getValue().getBytes()));
			}
		}

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC,
				(getBody().hashCode() % kafkaProperties.getPartitionCount()), getBody(), getBody(),
				new RecordHeaders(kafkaHeaders.toArray(new RecordHeader[kafkaHeaders.size()])));
		try {
			Future<SendResult<String, String>> futureObj = kafkaTemplate.send(record);
			futureObj.get();
		} catch (Exception exception) {

		}

		ConsumerRecord<String, String> received = consumedEventCache.get(100, TimeUnit.SECONDS);

		assertThat(received, hasValue(getBody()));

	}
}
