package com.springkafka;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.mockito.Mock;

import com.springkafka.api.ConsumedEventCache;

public class ConsumerEventCacheTest2 {
	
	@Mock
	private ConsumedEventCache mockConsumedEventCache;

	@Test
	public void testGet() {
		when(mockConsumedEventCache.get())
				.thenReturn(new ConsumerRecord<String, String>("abc", 0, 0, "key", "testdata"));

		ConsumerRecord<String, String> testData = mockConsumedEventCache.get();
		assertEquals("testdata", testData.value());
	}

}
