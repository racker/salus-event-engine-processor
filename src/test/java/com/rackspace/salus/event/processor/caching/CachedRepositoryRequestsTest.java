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

package com.rackspace.salus.event.processor.caching;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.Monitor;
import com.rackspace.salus.telemetry.entities.StateChange;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CachedRepositoryRequests.class})
@AutoConfigureCache(cacheProvider = CacheType.JCACHE)
public class CachedRepositoryRequestsTest {

  @Autowired
  CachedRepositoryRequests repositoryRequests;

  @MockBean
  MonitorRepository monitorRepository;

  @MockBean
  BoundMonitorRepository boundMonitorRepository;

  @MockBean
  StateChangeRepository stateChangeRepository;

  final PodamFactory podamFactory = new PodamFactoryImpl();


  @Test
  public void getMonitorIntervalTest() {
    when(monitorRepository.findByIdAndTenantId(any(), anyString()))
        .thenReturn(Optional.of(
            new Monitor().setInterval(Duration.ofMinutes(1))));

    String tenantId = RandomStringUtils.randomAlphanumeric(5);
    UUID monitorId = UUID.randomUUID();

    Duration interval = repositoryRequests.getMonitorInterval(tenantId, monitorId);

    // first request triggers a db lookup
    verify(monitorRepository).findByIdAndTenantId(monitorId, tenantId);
    assertThat(interval).isEqualTo(Duration.ofMinutes(1));

    interval = repositoryRequests.getMonitorInterval(tenantId, monitorId);

    // no db lookup on subsequent requests.
    verifyNoMoreInteractions(monitorRepository);
    assertThat(interval).isEqualTo(Duration.ofMinutes(1));
  }

  @Test
  public void getExpectedEventCountForMonitorTest() {
    when(boundMonitorRepository.countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        anyString(), any(), anyString()))
        .thenReturn(3);

    final SalusEnrichedMetric metric = podamFactory.manufacturePojo(SalusEnrichedMetric.class);

    int expectedCount = repositoryRequests.getExpectedEventCountForMonitor(metric);

    // first request triggers a db lookup
    verify(boundMonitorRepository).countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        metric.getResourceId(), metric.getMonitorId(), metric.getTenantId());
    assertThat(expectedCount).isEqualTo(3);

    expectedCount = repositoryRequests.getExpectedEventCountForMonitor(metric);

    // no db lookup on subsequent requests.
    verifyNoMoreInteractions(boundMonitorRepository);
    assertThat(expectedCount).isEqualTo(3);
  }

  @Test
  public void getPreviousKnownStateTest() {
    final StateChange stateChange = podamFactory.manufacturePojo(StateChange.class);

    when(stateChangeRepository.findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        anyString(), anyString(), any(), any()))
        .thenReturn(Optional.of(stateChange));

    String state = repositoryRequests.getPreviousKnownState(
        stateChange.getTenantId(),
        stateChange.getResourceId(),
        stateChange.getMonitorId(),
        stateChange.getTaskId());

    // first request triggers a db lookup
    verify(stateChangeRepository).findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        stateChange.getTenantId(),
        stateChange.getResourceId(),
        stateChange.getMonitorId(),
        stateChange.getTaskId()
    );
    assertThat(state).isEqualTo(stateChange.getState());

    state = repositoryRequests.getPreviousKnownState(
        stateChange.getTenantId(),
        stateChange.getResourceId(),
        stateChange.getMonitorId(),
        stateChange.getTaskId());

    // no db lookup on subsequent requests.
    verifyNoMoreInteractions(stateChangeRepository);
    assertThat(state).isEqualTo(stateChange.getState());


  }


}
