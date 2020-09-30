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

import static com.rackspace.salus.event.processor.utils.TestDataGenerators.createSalusEnrichedMetric;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.Monitor;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EsperEventsHandler.class, TaskWarmthTracker.class,
    SimpleMeterRegistry.class, ObjectMapper.class})
public class EsperEventsHandlerTest {

  @Autowired
  EsperEventsHandler eventsHandler;

  @Autowired
  TaskWarmthTracker warmthTracker;

  @MockBean
  EventProducer eventProducer;

  @MockBean
  MonitorRepository monitorRepository;

  @MockBean
  BoundMonitorRepository boundMonitorRepository;

  @MockBean
  StateChangeRepository stateChangeRepository;

  @Test
  public void processEsperEvents_noEvents() {
    eventsHandler.processEsperEvents(Collections.emptyList());
    verifyNoInteractions(stateChangeRepository, monitorRepository, boundMonitorRepository);
  }

  @Test
  public void processEsperEvents_oneExpectedEvent_noHistory() {

    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    // set monitor interval
    when(monitorRepository.findByIdAndTenantId(any(), anyString()))
        .thenReturn(Optional.of(
            new Monitor().setInterval(Duration.ofMinutes(1))));

    // set expected number of events/zones
    when(boundMonitorRepository.countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        anyString(), any(), anyString()))
        .thenReturn(1);

    // set state change history
    when(stateChangeRepository.findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        anyString(), anyString(), any(), any()))
        .thenReturn(Optional.empty());

    eventsHandler.processEsperEvents(List.of(metric));

    verify(monitorRepository)
        .findByIdAndTenantId(metric.getMonitorId(), metric.getTenantId());
    verify(boundMonitorRepository)
        .countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
            metric.getResourceId(), metric.getMonitorId(), metric.getTenantId());
    verify(stateChangeRepository)
        .findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
            metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(stateChangeRepository).save(any());
    verify(eventProducer).sendStateChange(any());
  }

  @Test
  public void processEsperEvents_threeExpectedEvents_noHistory() {

    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    // set monitor interval
    when(monitorRepository.findByIdAndTenantId(any(), anyString()))
        .thenReturn(Optional.of(
            new Monitor().setInterval(Duration.ofMinutes(1))));

    // set expected number of events/zones
    when(boundMonitorRepository.countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        anyString(), any(), anyString()))
        .thenReturn(3);

    // set state change history
    when(stateChangeRepository.findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        anyString(), anyString(), any(), any()))
        .thenReturn(Optional.empty());

    // first send a lesser number of events than expected
    eventsHandler.processEsperEvents(List.of(metric, metric));

    verify(monitorRepository)
        .findByIdAndTenantId(metric.getMonitorId(), metric.getTenantId());
    verify(boundMonitorRepository)
        .countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
            metric.getResourceId(), metric.getMonitorId(), metric.getTenantId());
    // no state change actions occur since the task is not "warm"
    verifyNoInteractions(stateChangeRepository);


    // now send the expected number of events
    eventsHandler.processEsperEvents(List.of(metric, metric, metric));

    verify(stateChangeRepository)
        .findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
            metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(stateChangeRepository).save(any());
    verify(eventProducer).sendStateChange(any());
  }
}
