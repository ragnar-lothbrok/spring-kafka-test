package com.springkafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafkaconfig")
public class KafkaProperties {

	private String topic;
	private String groupId;
	private Integer partitionCount;
	private Boolean autoCommitEnabled;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public Integer getPartitionCount() {
		return partitionCount;
	}

	public void setPartitionCount(Integer partitionCount) {
		this.partitionCount = partitionCount;
	}

	public Boolean getAutoCommitEnabled() {
		return autoCommitEnabled;
	}

	public void setAutoCommitEnabled(Boolean autoCommitEnabled) {
		this.autoCommitEnabled = autoCommitEnabled;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	private int retries;

}
