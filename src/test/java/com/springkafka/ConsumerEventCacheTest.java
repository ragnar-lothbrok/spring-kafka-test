package com.springkafka;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import com.springkafka.api.ConsumedEventCache;

public class ConsumerEventCacheTest {

	@Test
	public void testGet() {
		ConsumedEventCache mockConsumedEventCache = mock(ConsumedEventCache.class);
		when(mockConsumedEventCache.get())
				.thenReturn(new ConsumerRecord<String, String>("abc", 0, 0, "key", "testdata"));

		ConsumerRecord<String, String> testData = mockConsumedEventCache.get();
		assertEquals("testdata", testData.value());
	}

}
