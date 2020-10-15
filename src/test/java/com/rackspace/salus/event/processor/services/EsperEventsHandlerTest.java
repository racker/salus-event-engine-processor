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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.event.processor.caching.CachedRepositoryRequests;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.StateChange;
import com.rackspace.salus.telemetry.model.ConfigSelectorScope;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EsperEventsHandler.class, TaskWarmthTracker.class,
    SimpleMeterRegistry.class, ObjectMapper.class, CachedRepositoryRequests.class})
// caching is tested in CachedRepositoryRequestsTest not here
@AutoConfigureCache(cacheProvider = CacheType.NONE)
public class EsperEventsHandlerTest {

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  EsperEventsHandler eventsHandler;

  @Autowired
  TaskWarmthTracker warmthTracker;

  @MockBean
  CachedRepositoryRequests cachedRepositoryRequests;

  // these are required due to ScopedProxyMode in cachedRepositoryRequests
  @MockBean
  BoundMonitorRepository boundMonitorRepository;
  @MockBean
  MonitorRepository monitorRepository;
  @MockBean
  StateChangeRepository stateChangeRepository;

  @MockBean
  EventProducer eventProducer;

  @Captor
  ArgumentCaptor<StateChange> stateChangeCaptor;

  @Before
  public void setup() {
    // set default monitor interval
    when(cachedRepositoryRequests.getMonitorInterval(anyString(), any()))
        .thenReturn(Duration.ofMinutes(1));

    // set default expected events count
    when(cachedRepositoryRequests.getExpectedEventCountForMonitor(any()))
        .thenReturn(1);

    // set default empty state change history
    when(cachedRepositoryRequests.getPreviousKnownState(anyString(), anyString(), any(), any()))
        .thenReturn(null);

    when(cachedRepositoryRequests.saveAndCacheStateChange(any()))
        .then(returnsFirstArg());
  }

  @Test
  public void processEsperEvents_noEvents() {
    eventsHandler.processEsperEvents(Collections.emptyList());
    verifyNoInteractions(cachedRepositoryRequests);
  }

  @Test
  public void processEsperEvents_oneExpectedEvent_noHistory() throws JsonProcessingException {
    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    StateChange expectedStateChange = new StateChange()
        .setPreviousState(null)
        .setState(metric.getState())
        .setContributingEvents(objectMapper.writeValueAsString(List.of(metric)))
        .setMessage(null)
        .setTenantId(metric.getTenantId())
        .setResourceId(metric.getResourceId())
        .setMonitorId(metric.getMonitorId())
        .setTaskId(metric.getTaskId());

    eventsHandler.processEsperEvents(List.of(metric));

    verify(cachedRepositoryRequests)
        .getMonitorInterval(metric.getTenantId(), metric.getMonitorId());
    verify(cachedRepositoryRequests)
        .getExpectedEventCountForMonitor(metric);
    verify(cachedRepositoryRequests)
        .getPreviousKnownState(
            metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(cachedRepositoryRequests).saveAndCacheStateChange(stateChangeCaptor.capture());
    StateChange stateChange = stateChangeCaptor.getValue();
    assertThat(stateChange)
        .isEqualToIgnoringGivenFields(expectedStateChange,
            "id", "creationTimestamp", "evaluationTimestamp", "updatedTimestamp");

    verify(eventProducer).sendStateChange(stateChange);
  }

  @Test
  public void processEsperEvents_threeExpectedEvents_noHistory() throws JsonProcessingException {
    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    StateChange expectedStateChange = new StateChange()
        .setPreviousState(null)
        .setState(metric.getState())
        .setContributingEvents(objectMapper.writeValueAsString(List.of(metric, metric, metric)))
        .setMessage(null)
        .setTenantId(metric.getTenantId())
        .setResourceId(metric.getResourceId())
        .setMonitorId(metric.getMonitorId())
        .setTaskId(metric.getTaskId());

    // override default expected number of events/zones
    when(cachedRepositoryRequests.getExpectedEventCountForMonitor(any()))
        .thenReturn(3);

    // first send a lesser number of events than expected
    eventsHandler.processEsperEvents(List.of(metric, metric));

    verify(cachedRepositoryRequests).getMonitorInterval(metric.getTenantId(), metric.getMonitorId());
    verify(cachedRepositoryRequests).getExpectedEventCountForMonitor(metric);
    // no state change actions occur since the task is not "warm"
    verifyNoMoreInteractions(cachedRepositoryRequests, eventProducer);

    // now send the expected number of events
    eventsHandler.processEsperEvents(List.of(metric, metric, metric));

    verify(cachedRepositoryRequests)
        .getPreviousKnownState(
            metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(cachedRepositoryRequests).saveAndCacheStateChange(stateChangeCaptor.capture());
    StateChange stateChange = stateChangeCaptor.getValue();
    assertThat(stateChange)
        .isEqualToIgnoringGivenFields(expectedStateChange,
            "id", "creationTimestamp", "evaluationTimestamp", "updatedTimestamp");

    verify(eventProducer).sendStateChange(stateChange);
  }

  @Test
  public void processEsperEvents_verifyStateChange() throws JsonProcessingException {
    // these represent the same monitor/zone outputting different states on subsequent polling cycles
    SalusEnrichedMetric metric1 = createSalusEnrichedMetric().setState("oldState");
    SalusEnrichedMetric metric2 = SerializationUtils.clone(metric1).setState("newState");

    StateChange expectedStateChange1 = new StateChange()
        .setPreviousState(null)
        .setState(metric1.getState())
        .setContributingEvents(objectMapper.writeValueAsString(List.of(metric1)))
        .setMessage(null)
        .setTenantId(metric1.getTenantId())
        .setResourceId(metric1.getResourceId())
        .setMonitorId(metric1.getMonitorId())
        .setTaskId(metric1.getTaskId());

    StateChange expectedStateChange2 = new StateChange()
        .setPreviousState("oldState")
        .setState("newState")
        .setContributingEvents(objectMapper.writeValueAsString(List.of(metric2)))
        .setMessage(null)
        .setTenantId(metric1.getTenantId())
        .setResourceId(metric1.getResourceId())
        .setMonitorId(metric1.getMonitorId())
        .setTaskId(metric1.getTaskId());

    // override the default mock
    when(cachedRepositoryRequests.getPreviousKnownState(anyString(), anyString(), any(), any()))
        .thenReturn(null)
        .thenReturn("oldState");

    // send two events each with different states
    eventsHandler.processEsperEvents(List.of(metric1));
    eventsHandler.processEsperEvents(List.of(metric2));

    verify(cachedRepositoryRequests, times(2)).saveAndCacheStateChange(stateChangeCaptor.capture());
    List<StateChange> stateChanges = stateChangeCaptor.getAllValues();
    assertThat(stateChanges)
        .usingElementComparatorIgnoringFields("id", "creationTimestamp", "evaluationTimestamp", "updatedTimestamp")
        .containsExactlyInAnyOrder(expectedStateChange1, expectedStateChange2);

    verify(eventProducer).sendStateChange(stateChanges.get(0));
    verify(eventProducer).sendStateChange(stateChanges.get(1));

    // verify basic db/cache lookups
    verify(cachedRepositoryRequests, times(2)).getMonitorInterval(metric1.getTenantId(), metric1.getMonitorId());
    verify(cachedRepositoryRequests).getExpectedEventCountForMonitor(metric1);
    verify(cachedRepositoryRequests).getExpectedEventCountForMonitor(metric2);
    verify(cachedRepositoryRequests, times(2)).getPreviousKnownState(
        metric1.getTenantId(), metric1.getResourceId(), metric1.getMonitorId(), metric1.getTaskId());

    verifyNoMoreInteractions(cachedRepositoryRequests, eventProducer);
  }

  @Test
  public void processEsperEvents_localEvent() throws JsonProcessingException {
    SalusEnrichedMetric metric = createSalusEnrichedMetric().setMonitorSelectorScope(
        ConfigSelectorScope.LOCAL.toString());

    StateChange expectedStateChange = new StateChange()
        .setPreviousState(null)
        .setState(metric.getState())
        .setContributingEvents(objectMapper.writeValueAsString(List.of(metric)))
        .setMessage(null)
        .setTenantId(metric.getTenantId())
        .setResourceId(metric.getResourceId())
        .setMonitorId(metric.getMonitorId())
        .setTaskId(metric.getTaskId());

    eventsHandler.processEsperEvents(List.of(metric));

    // no requests were made to look up the expected event count since (local monitors are always 1)
    verify(cachedRepositoryRequests, never()).getExpectedEventCountForMonitor(metric);

    // other requests were still made
    verify(cachedRepositoryRequests).getMonitorInterval(metric.getTenantId(), metric.getMonitorId());
    verify(cachedRepositoryRequests).getPreviousKnownState(
        metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(cachedRepositoryRequests).saveAndCacheStateChange(stateChangeCaptor.capture());
    StateChange stateChange = stateChangeCaptor.getValue();
    assertThat(stateChange)
        .isEqualToIgnoringGivenFields(expectedStateChange,
            "id", "creationTimestamp", "evaluationTimestamp", "updatedTimestamp");

    verify(eventProducer).sendStateChange(stateChange);

    verifyNoMoreInteractions(cachedRepositoryRequests, eventProducer);
  }
}
