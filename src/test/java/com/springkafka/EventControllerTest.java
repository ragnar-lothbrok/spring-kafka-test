package com.springkafka;

import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.springkafka.api.ConsumedEventCache;
import com.springkafka.api.EventProducer;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@WebAppConfiguration
public class EventControllerTest {

	@Autowired
	private EventProducer eventProducer;

	@Autowired
	private ConsumedEventCache consumedEventCache;

	private static final String TOPIC = "test_one";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, TOPIC);

	private MockMvc mockMvc;

	@Autowired
	private WebApplicationContext wac;

	@Before
	public void setup() throws Exception {
		this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
	}

	public Map<String, String> getHeader() {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("ID", UUID.randomUUID().toString());
		return headers;
	}

	@Test
	public void givenWac_whenServletContext_thenItProvidesGreetController() {
		ServletContext servletContext = wac.getServletContext();

		Assert.assertNotNull(servletContext);
		Assert.assertTrue(servletContext instanceof MockServletContext);
		Assert.assertNotNull(wac.getBean("eventController"));
	}

	@Test
	public void givenGreetURI_whenMockMVC_thenVerifyResponse() throws Exception {
		MvcResult mvcResult = this.mockMvc.perform(get("/events")).andDo(print()).andExpect(status().isOk())
				.andReturn();

		Assert.assertEquals("", mvcResult.getResponse().getContentAsString());
	}

	public String getBody() {
		return "TEST DATA";
	}
	
	@Test
	public void testSendUsingController() throws Exception {
		eventProducer.sendEvent(TOPIC, getHeader(), getBody());

		MvcResult mvcResult = this.mockMvc.perform(get("/events")).andDo(print()).andExpect(status().isOk())
				.andReturn();

		Assert.assertEquals(getBody(), mvcResult.getResponse().getContentAsString());

	}

	@Test
	public void testSend() throws InterruptedException {
		eventProducer.sendEvent(TOPIC, getHeader(), getBody());

		ConsumerRecord<String, String> received = consumedEventCache.get(100, TimeUnit.SECONDS);

		assertThat(received, hasValue(getBody()));

	}

}
