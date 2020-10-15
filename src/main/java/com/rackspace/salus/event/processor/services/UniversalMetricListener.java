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

import com.google.common.collect.Sets;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.monplat.protocol.UniversalMetricFrame.MonitoringSystem;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
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

  // todo make this a config parameter
  public static final Duration partitionAssignmentDelay = Duration.ofSeconds(5);

  private Set<Integer> trackedPartitions;
  private ScheduledFuture<?> scheduledPartitionChangeTask;
  private final String consumerId;

  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  private final UniversalMetricHandler handler;
  private final String topic;
  private final KafkaListenerEndpointRegistry registry;

  @Autowired
  public UniversalMetricListener(
      UniversalMetricHandler handler,
      KafkaTopicProperties properties,
      KafkaListenerEndpointRegistry registry) {
    this.handler = handler;
    this.topic = properties.getMetrics();
    this.registry = registry;
    this.trackedPartitions = Sets.newConcurrentHashSet();
    this.consumerId = RandomStringUtils.randomAlphabetic(10);
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
   * @return The unique consumerId
   */
  public String getConsumerId() {
    return consumerId;
  }

  /**
   * This receives a UniversalMetricFrame event from Kafka and passes it on to
   * another service to process it.
   *
   * @param metric The UniversalMetricFrame read from Kafka.
   */
  @KafkaListener(
      autoStartup = "${salus.kafka-listener-auto-start:true}",
      id = "#{__listener.consumerId}",
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
        getConsumerId(),
        assignments.keySet().stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet()));

    schedulePartitionReload();
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.debug("Partitions Revoked from consumer={}: {}",
        getConsumerId(),
        partitions.stream()
            .map(TopicPartition::partition)
            .collect(Collectors.toSet()));

    schedulePartitionReload();
  }

  private void schedulePartitionReload() {
    if (scheduledPartitionChangeTask != null && !scheduledPartitionChangeTask.isDone()) {
      scheduledPartitionChangeTask.cancel(false);
    }
    scheduledPartitionChangeTask = executorService.schedule(
        this::reloadPartitions,
        partitionAssignmentDelay.toSeconds(),
        TimeUnit.SECONDS);
  }

  /**
   * Determine which partitions were removed and which were added.
   *
   * Remove deployed tasks for the removed partitions.
   * Deploy new tasks for the added partitions.
   */
  private void reloadPartitions() {
    log.info("Reloading changed partitions");
    Set<Integer> assignedPartitions = getCurrentAssignedPartitions();

    Set<Integer> removedPartitions = new HashSet<>(trackedPartitions);
    removedPartitions.removeAll(assignedPartitions);

    Set<Integer> addedPartitions = new HashSet<>(assignedPartitions);
    addedPartitions.removeAll(trackedPartitions);

    trackedPartitions = assignedPartitions;

    handler.removeTasksForPartitions(removedPartitions);
    handler.deployTasksForPartitions(addedPartitions);
  }

  Set<Integer> getCurrentAssignedPartitions() {
    MessageListenerContainer container = registry.getListenerContainer(consumerId);
    if (container == null) {
      return Collections.emptySet();
    }
    Collection<TopicPartition> assignments = container.getAssignedPartitions();
    if (assignments == null) {
      return Collections.emptySet();
    }

    return assignments.stream()
        .map(TopicPartition::partition)
        .collect(Collectors.toSet());
  }

  Set<Integer> getTrackedPartitions() {
    return this.trackedPartitions;
  }

  void stop() {
    this.registry.getListenerContainer(consumerId).stop();
  }

  void start() {
    this.registry.getListenerContainer(consumerId).start();
  }
}
