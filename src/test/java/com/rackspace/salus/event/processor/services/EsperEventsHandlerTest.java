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
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.event.processor.caching.CachedRepositoryRequests;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EsperEventsHandler.class, TaskWarmthTracker.class,
    SimpleMeterRegistry.class, ObjectMapper.class, CachedRepositoryRequests.class})
// caching is tested in CachedRepositoryRequestsTest not here
@AutoConfigureCache(cacheProvider = CacheType.NONE)
public class EsperEventsHandlerTest {

  @Autowired
  EsperEventsHandler eventsHandler;

  @Autowired
  TaskWarmthTracker warmthTracker;

  @MockBean
  CachedRepositoryRequests cachedRepositoryRequests;

  @MockBean
  EventProducer eventProducer;

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
  public void processEsperEvents_oneExpectedEvent_noHistory() {
    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    eventsHandler.processEsperEvents(List.of(metric));

    verify(cachedRepositoryRequests)
        .getMonitorInterval(metric.getTenantId(), metric.getMonitorId());
    verify(cachedRepositoryRequests)
        .getExpectedEventCountForMonitor(metric);
    verify(cachedRepositoryRequests)
        .getPreviousKnownState(
            metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verify(cachedRepositoryRequests).saveAndCacheStateChange(any());
    verify(eventProducer).sendStateChange(any());
  }

  @Test
  public void processEsperEvents_threeExpectedEvents_noHistory() {
    SalusEnrichedMetric metric = createSalusEnrichedMetric();

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

    verify(cachedRepositoryRequests).saveAndCacheStateChange(any());
    verify(eventProducer).sendStateChange(any());
  }

  @Test
  public void processEsperEvents_verifyStateChange() {
    SalusEnrichedMetric metric = createSalusEnrichedMetric();

    // send two events each with different states
    eventsHandler.processEsperEvents(List.of(metric.setState("oldState")));
    eventsHandler.processEsperEvents(List.of(metric.setState("newState")));

    verify(cachedRepositoryRequests, times(2)).saveAndCacheStateChange(any());
    verify(eventProducer, times(2)).sendStateChange(any());

    // verify basic db/cache lookups
    verify(cachedRepositoryRequests, times(2)).getMonitorInterval(metric.getTenantId(), metric.getMonitorId());
    verify(cachedRepositoryRequests, times(2)).getExpectedEventCountForMonitor(metric);
    verify(cachedRepositoryRequests, times(2)).getPreviousKnownState(
        metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId());

    verifyNoMoreInteractions(cachedRepositoryRequests, eventProducer);
  }
}
