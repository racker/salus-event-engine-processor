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

import static com.rackspace.salus.event.processor.config.CacheConfig.EXPECTED_EVENT_COUNTS;
import static com.rackspace.salus.event.processor.config.CacheConfig.MONITOR_INTERVALS;
import static com.rackspace.salus.event.processor.config.CacheConfig.STATE_HISTORY;

import com.rackspace.salus.event.processor.config.CacheConfig;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.Monitor;
import com.rackspace.salus.telemetry.entities.StateChange;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import java.time.Duration;
import java.util.UUID;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

@Component
@Import(CacheConfig.class)
// this is required to allow a method to call @Cacheable methods from within the same class
// i.e. how saveAndCacheStateChange calls updateStateChangeCache.
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class CachedRepositoryRequests {

  private final BoundMonitorRepository boundMonitorRepository;
  private final StateChangeRepository stateChangeRepository;
  private final MonitorRepository monitorRepository;

  private final CachedRepositoryRequests _cachedRequests; // related to ScopedProxyMode

  private static final Duration FALLBACK_MONITOR_INTERVAL = Duration.ofMinutes(30);

  public CachedRepositoryRequests(
      BoundMonitorRepository boundMonitorRepository,
      StateChangeRepository stateChangeRepository,
      MonitorRepository monitorRepository,
      CachedRepositoryRequests cachedRequests) {
    this.boundMonitorRepository = boundMonitorRepository;
    this.stateChangeRepository = stateChangeRepository;
    this.monitorRepository = monitorRepository;
    _cachedRequests = cachedRequests;
  }

  @Cacheable(cacheNames = MONITOR_INTERVALS, key = "{#tenantId, #monitorId}")
  public Duration getMonitorInterval(String tenantId, UUID monitorId) {
    return monitorRepository.findByIdAndTenantId(monitorId, tenantId)
        .map(Monitor::getInterval)
        .orElse(FALLBACK_MONITOR_INTERVAL);
  }

  @Cacheable(cacheNames = EXPECTED_EVENT_COUNTS,
      key = "{#metric.tenantId, #metric.resourceId, #metric.monitorId}")
  public int getExpectedEventCountForMonitor(SalusEnrichedMetric metric) {
    return boundMonitorRepository.countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        metric.getResourceId(),
        metric.getMonitorId(),
        metric.getTenantId());
  }

  @Cacheable(cacheNames = STATE_HISTORY,
      key = "{#tenantId, #resourceId, #monitorId, #taskId}")
  public String getPreviousKnownState(String tenantId, String resourceId, UUID monitorId, UUID taskId) {
    return stateChangeRepository.findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        tenantId, resourceId, monitorId, taskId)
        .map(StateChange::getState).orElse(null);
    // TODO : return UNKNOWN default state?
  }

  public StateChange saveAndCacheStateChange(StateChange stateChange) {
    _cachedRequests.updateStateChangeCache(stateChange);
    return stateChangeRepository.save(stateChange);
  }

  @CachePut(cacheNames = STATE_HISTORY,
      key = "{#stateChange.tenantId, #stateChange.resourceId, #stateChange.monitorId, #stateChange.taskId}")
  public String updateStateChangeCache(StateChange stateChange) {
    return stateChange.getState();
  }
}
