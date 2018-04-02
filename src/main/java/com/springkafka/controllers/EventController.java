package com.springkafka.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.springkafka.api.ConsumedEventCache;

@RestController
public class EventController {

	@Autowired
	private ConsumedEventCache consumedEventCache;

	@RequestMapping(value = "/events", method = RequestMethod.GET)
	public String getEvents(HttpServletRequest request, HttpServletResponse response) {
		ConsumerRecord<String, String> event = consumedEventCache.get();
		if (event != null)
			return event.value();
		return null;
	}

}
