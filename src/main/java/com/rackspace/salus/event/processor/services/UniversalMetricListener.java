/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.event.processor.services;

import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.monplat.protocol.UniversalMetricFrame.MonitoringSystem;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.processor.config.AppProperties;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UniversalMetricListener implements ConsumerSeekAware {

  private final String listenerId;
  private final UniversalMetricHandler handler;
  private final String topic;
  private final KafkaListenerEndpointRegistry registry;
  private final PartitionTracker partitionTracker;

  @Autowired
  public UniversalMetricListener(
      AppProperties appProperties,
      UniversalMetricHandler handler,
      KafkaTopicProperties properties,
      KafkaListenerEndpointRegistry registry,
      PartitionTracker partitionTracker) {
    this.handler = handler;
    this.topic = properties.getMetrics();
    this.registry = registry;
    this.partitionTracker = partitionTracker;
    this.listenerId = appProperties.getMetricsConsumerListenerId();
  }

  /**
   * This method is used by the __listener.topic magic in the KafkaListener
   *
   * @return The topic to consume
   */
  public String getTopic() {
    return this.topic;
  }

  /**
   * This method is used by the __listener.topic magic in the KafkaListener
   *
   * @return The unique listenerId
   */
  public String getListenerId() {
    return listenerId;
  }

  /**
   * This receives a UniversalMetricFrame event from Kafka and passes it on to
   * another service to process it.
   *
   * @param metric The UniversalMetricFrame read from Kafka.
   */
  @KafkaListener(
      autoStartup = "${salus.event-processor.kafka-listener-auto-start:true}",
      id = "#{__listener.listenerId}",
      groupId = "${spring.kafka.consumer.group-id}",
      topics = "#{__listener.topic}",
      // override the default partition assignor to limit reassignment overhead
      properties = {"partition.assignment.strategy=org.apache.kafka.clients.consumer.StickyAssignor"})
  public void consumeUniversalMetrics(UniversalMetricFrame metric) {
    log.debug("Processing kapacitor event: {}", metric);
    if (metric.getMonitoringSystem().equals(MonitoringSystem.SALUS)) {
      handler.processSalusMetricFrame(metric);
    }
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
      ConsumerSeekCallback callback) {
    log.debug("Partitions Assigned to consumer={}: {}",
        getListenerId(),
        assignments.keySet().stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet()));

    partitionTracker.schedulePartitionReload();
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.debug("Partitions Revoked from consumer={}: {}",
        getListenerId(),
        partitions.stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet()));

    partitionTracker.schedulePartitionReload();
  }

  /**
   * Currently only used in tests.
   */
  void stop() {
    MessageListenerContainer container = registry.getListenerContainer(listenerId);
    if (container != null) {
      log.info("Stopping kafka listener container id={}", listenerId);
      container.stop();
    } else {
      log.error("Could not stop kafka listener. No container found with id={}", listenerId);
    }
  }

  /**
   * Currently only used in tests.
   */
  void start() {
    MessageListenerContainer container = registry.getListenerContainer(listenerId);
    if (container != null) {
      log.info("Starting kafka listener container id={}", listenerId);
      container.start();
    } else {
      log.error("Could not start kafka listener. No container found with id={}", listenerId);
    }
  }
}
