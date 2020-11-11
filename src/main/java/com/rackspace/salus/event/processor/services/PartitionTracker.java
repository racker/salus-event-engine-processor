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
import com.rackspace.salus.event.processor.config.AppProperties;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PartitionTracker {

  private final String listenerId;
  private final UniversalMetricHandler handler;
  private final KafkaListenerEndpointRegistry registry;

  // the time to wait before (un)deploying tasks from old/new assigned partitions
  public final Duration partitionAssignmentDelay;

  private Set<Integer> trackedPartitions;
  private ScheduledFuture<?> scheduledPartitionChangeTask;
  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

  public PartitionTracker(
      AppProperties appProperties,
      UniversalMetricHandler handler,
      KafkaListenerEndpointRegistry registry) {
    this.handler = handler;
    this.registry = registry;
    this.trackedPartitions = Sets.newConcurrentHashSet();
    this.partitionAssignmentDelay = appProperties.getPartitionAssignmentDelay();
    this.listenerId = appProperties.getMetricsConsumerListenerId();
  }

  void schedulePartitionReload() {
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
    MessageListenerContainer container = registry.getListenerContainer(listenerId);
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

  /**
   * Get the set of partitions the processor is handling tasks for.
   *
   * @return A set of partition ids.
   */
  Set<Integer> getTrackedPartitions() {
    return this.trackedPartitions;
  }
}
