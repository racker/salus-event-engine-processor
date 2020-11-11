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

import static com.rackspace.salus.telemetry.messaging.KafkaMessageKeyBuilder.buildMessageKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.processor.caching.CachedRepositoryRequests;
import com.rackspace.salus.event.processor.config.AppProperties;
import com.rackspace.salus.event.processor.engine.EsperEngine;
import com.rackspace.salus.telemetry.entities.BoundMonitor;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.subtype.SalusEventEngineTask;
import com.rackspace.salus.telemetry.messaging.MonitorChangeEvent;
import com.rackspace.salus.telemetry.messaging.TaskChangeEvent;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@Slf4j
@RunWith(SpringRunner.class)
@EmbeddedKafka(partitions = 1,
    topics = {"telemetry.monitor-changes.json", "telemetry.task-changes.json"})
@SpringBootTest(
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest"},
    classes = {
        ChangeEventsListener.class,
        KafkaAutoConfiguration.class, AppProperties.class, KafkaTopicProperties.class})
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class ChangeEventListenerTest {

  final PodamFactory podamFactory = new PodamFactoryImpl();

  @Autowired
  KafkaTemplate<String, Object> kafkaTemplate;

  @Autowired
  KafkaTopicProperties topicProperties;

  @MockBean
  BoundMonitorRepository boundMonitorRepository;

  @MockBean
  EventEngineTaskRepository taskRepository;

  @MockBean
  CachedRepositoryRequests cachedRepositoryRequests;

  @MockBean
  EsperEngine esperEngine;

  @MockBean
  PartitionTracker partitionTracker;

  @Captor
  ArgumentCaptor<Collection<BoundMonitor>> cacheCaptor;

  @Test
  public void testConsumeMonitorChangeEvents() {
    String tenantId = RandomStringUtils.randomAlphabetic(5);
    UUID monitorId = UUID.randomUUID();

    MonitorChangeEvent event = new MonitorChangeEvent()
        .setMonitorId(monitorId)
        .setTenantId(tenantId);

    List<BoundMonitor> boundMonitors = podamFactory.manufacturePojo(ArrayList.class, BoundMonitor.class);
    Page<BoundMonitor> firstPage = new PageImpl<>(boundMonitors.subList(0, 3), PageRequest.of(0, 3), 5);
    Page<BoundMonitor> secondPage = new PageImpl<>(boundMonitors.subList(3, 5), PageRequest.of(1, 2), 5);

    when(boundMonitorRepository.findAllByMonitor_IdAndMonitor_TenantId(any(), anyString(), any()))
        .thenReturn(firstPage)
        .thenReturn(secondPage)
        .thenReturn(Page.empty());

    kafkaTemplate.send(topicProperties.getMonitorChanges(), buildMessageKey(event), event);

    // use a timeout to allow for consumer startup
    verify(cachedRepositoryRequests, timeout(5000)).clearMonitorIntervalCache(tenantId, monitorId);

    // handled first page then second page
    verify(cachedRepositoryRequests, times(2)).clearExpectedCountCache(cacheCaptor.capture());
    List<Collection<BoundMonitor>> values = cacheCaptor.getAllValues();
    assertThat(values.get(0)).hasSize(3);
    assertThat(values.get(1)).hasSize(2);

    PageRequest pageRequest = PageRequest.of(0,1000);
    verify(boundMonitorRepository).findAllByMonitor_IdAndMonitor_TenantId(monitorId, tenantId, pageRequest);
    verify(boundMonitorRepository).findAllByMonitor_IdAndMonitor_TenantId(monitorId, tenantId, pageRequest.next());
    verify(boundMonitorRepository).findAllByMonitor_IdAndMonitor_TenantId(monitorId, tenantId, pageRequest.next().next());

    verifyNoMoreInteractions(cachedRepositoryRequests, boundMonitorRepository);
  }

  @Test
  public void testConsumeTaskChangeEvents() {
    String tenantId = RandomStringUtils.randomAlphabetic(5);
    UUID taskId = UUID.randomUUID();

    EventEngineTask task = new SalusEventEngineTask()
        .setId(taskId)
        .setTenantId(tenantId);

    when(partitionTracker.getTrackedPartitions()).thenReturn(Set.of(0, 5));
    when(taskRepository.findByTenantIdAndId(anyString(), any())).thenReturn(Optional.of(task));

    TaskChangeEvent event = new TaskChangeEvent()
        .setPartitionId(5)
        .setTaskId(taskId)
        .setTenantId(tenantId);

    kafkaTemplate.send(topicProperties.getTaskChanges(), buildMessageKey(event), event);

    // use a timeout to allow for consumer startup
    verify(partitionTracker, timeout(5000)).getTrackedPartitions();
    verify(taskRepository).findByTenantIdAndId(event.getTenantId(), event.getTaskId());
    verify(esperEngine).removeTask(event.getTenantId(), event.getTaskId());
    verify(esperEngine).addTask(task);

    verifyNoMoreInteractions(esperEngine, partitionTracker, taskRepository);
  }

  @Test
  public void testConsumeTaskChangeEvents_nonTrackedPartition() {
    String tenantId = RandomStringUtils.randomAlphabetic(5);
    UUID taskId = UUID.randomUUID();

    when(partitionTracker.getTrackedPartitions()).thenReturn(Set.of(0, 5));

    TaskChangeEvent event = new TaskChangeEvent()
        .setPartitionId(100) // set a partition that is not tracked
        .setTaskId(taskId)
        .setTenantId(tenantId);

    kafkaTemplate.send(topicProperties.getTaskChanges(), buildMessageKey(event), event);

    // use a timeout to allow for consumer startup
    verify(partitionTracker, timeout(5000)).getTrackedPartitions();
    verify(esperEngine, never()).removeTask(anyString(), any());
    verify(esperEngine, never()).addTask(any());

    verifyNoMoreInteractions(esperEngine, partitionTracker, taskRepository);
  }

  @Test
  public void testConsumeTaskChangeEvents_removedTask() {
    String tenantId = RandomStringUtils.randomAlphabetic(5);
    UUID taskId = UUID.randomUUID();

    when(partitionTracker.getTrackedPartitions()).thenReturn(Set.of(0, 5));
    when(taskRepository.findByTenantIdAndId(anyString(), any())).thenReturn(Optional.empty());

    TaskChangeEvent event = new TaskChangeEvent()
        .setPartitionId(5)
        .setTaskId(taskId)
        .setTenantId(tenantId);

    kafkaTemplate.send(topicProperties.getTaskChanges(), buildMessageKey(event), event);

    // use a timeout to allow for consumer startup
    verify(partitionTracker, timeout(5000)).getTrackedPartitions();
    verify(taskRepository).findByTenantIdAndId(event.getTenantId(), event.getTaskId());
    verify(esperEngine).removeTask(event.getTenantId(), event.getTaskId());
    verify(esperEngine, never()).addTask(any());

    verifyNoMoreInteractions(esperEngine, partitionTracker, taskRepository);
  }
}
