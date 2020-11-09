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

import com.rackspace.salus.event.processor.caching.CachedRepositoryRequests;
import com.rackspace.salus.event.processor.engine.EsperEngine;
import com.rackspace.salus.telemetry.entities.BoundMonitor;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.messaging.MonitorChangeEvent;
import com.rackspace.salus.telemetry.messaging.TaskChangeEvent;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ChangeEventsListener {

  private final PartitionTracker partitionTracker;
  private final EventEngineTaskRepository taskRepository;
  private final EsperEngine esperEngine;
  private final CachedRepositoryRequests cachedRepositoryRequests;
  private final BoundMonitorRepository boundMonitorRepository;

  public ChangeEventsListener(
      PartitionTracker partitionTracker,
      EventEngineTaskRepository taskRepository,
      EsperEngine esperEngine,
      CachedRepositoryRequests cachedRepositoryRequests,
      BoundMonitorRepository boundMonitorRepository) {
    this.partitionTracker = partitionTracker;
    this.taskRepository = taskRepository;
    this.esperEngine = esperEngine;
    this.cachedRepositoryRequests = cachedRepositoryRequests;
    this.boundMonitorRepository = boundMonitorRepository;
  }

  /**
   * Consume monitor change events and evict any relevant cached monitor data.
   *
   * @param event Basic details of the monitor that has changed.
   */
  @KafkaListener
  public void consumeMonitorChangeEvents(MonitorChangeEvent event) {

    Pageable pageRequest = PageRequest.of(0, 1000);
    Page<BoundMonitor> boundMonitors = boundMonitorRepository
        .findAllByMonitor_IdAndMonitor_TenantId(event.getMonitorId(), event.getTenantId(), pageRequest);

    while (boundMonitors.hasContent()) {
      // try to limit cache evict calls by filtering the duplicate keys.
      // each of these will already contain the same tenantId and monitorId so we only need to
      // perform a distinct operation on the resourceId.
      Collection<BoundMonitor> uniqueKeys = boundMonitors.get()
          .collect(Collectors.groupingBy(BoundMonitor::getResourceId,
              Collectors.collectingAndThen(
                  Collectors.toList(),
                  monitors -> monitors.get(0))))
          .values();

      cachedRepositoryRequests.clearExpectedCountCache(uniqueKeys);

      pageRequest = pageRequest.next();
      boundMonitors = boundMonitorRepository
          .findAllByMonitor_IdAndMonitor_TenantId(event.getMonitorId(), event.getTenantId(), pageRequest);
    }
  }

  /**
   * Consume task change events, undeploy any deleted tasks and redeploy any new/updated tasks.
   *
   * @param event Basic details of the task that has changed.
   */
  @KafkaListener
  public void consumeTaskChangeEvents(TaskChangeEvent event) {
    if (!isPartitionAssigned(event.getPartitionId())) {
      return;
    }

    esperEngine.removeTask(event.getTenantId(), event.getTaskId());

    Optional<EventEngineTask> task = taskRepository.findByTenantIdAndId(event.getTenantId(), event.getTaskId());
    if (task.isEmpty()) {
      return;
    }
    esperEngine.addTask(task.get());
  }

  /**
   * Test whether a partition is assigned to this consumer or not.
   *
   * @param partitionId The partitionId to verify.
   * @return True if it is assigned, otherwise false.
   */
  private boolean isPartitionAssigned(int partitionId) {
    return partitionTracker.getTrackedPartitions().contains(partitionId);
  }

}
