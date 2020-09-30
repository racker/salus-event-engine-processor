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

import static com.rackspace.salus.event.processor.config.CacheConfig.EXPECTED_EVENT_COUNTS;
import static com.rackspace.salus.event.processor.config.CacheConfig.MONITOR_INTERVALS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.Monitor;
import com.rackspace.salus.telemetry.entities.StateChange;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EsperEventsHandler {

  private final BoundMonitorRepository boundMonitorRepository;
  private final StateChangeRepository stateChangeRepository;
  private final MonitorRepository monitorRepository;

  private final MeterRegistry meterRegistry;
  private final ObjectMapper objectMapper;

  private final EventProducer eventProducer;
  private final TaskWarmthTracker taskWarmthTracker;



  private static final Duration DEFAULT_METRIC_WINDOW = Duration.ofMinutes(30);
  private static final double METRIC_WINDOW_MULTIPLIER = 1.5;

  private final Counter.Builder eventProcessingAnomalies;

  @Autowired
  public EsperEventsHandler(
      BoundMonitorRepository boundMonitorRepository,
      StateChangeRepository stateChangeRepository,
      MonitorRepository monitorRepository,
      MeterRegistry meterRegistry, ObjectMapper objectMapper,
      EventProducer eventProducer,
      TaskWarmthTracker taskWarmthTracker,
      @Value("${spring.application.name}") String appName) {
    this.boundMonitorRepository = boundMonitorRepository;
    this.stateChangeRepository = stateChangeRepository;
    this.monitorRepository = monitorRepository;
    this.meterRegistry = meterRegistry;
    this.objectMapper = objectMapper;
    this.eventProducer = eventProducer;
    this.taskWarmthTracker = taskWarmthTracker;

    eventProcessingAnomalies = Counter.builder("event_processing_anomaly")
        .tag(MetricTags.SERVICE_METRIC_TAG, appName);
  }

  public void processEsperEvents(List<SalusEnrichedMetric> metricsWithState) {
    if (metricsWithState.size() == 0) {
      log.warn("Unable to process empty metrics list");
      eventProcessingAnomalies.tags(MetricTags.OPERATION_METRIC_TAG, "processEsperEvents",
          MetricTags.REASON, "empty_metrics_list")
          .register(meterRegistry).increment();
      return;
    }

    String tenantId = metricsWithState.get(0).getTenantId();
    String resourceId = metricsWithState.get(0).getResourceId();
    UUID monitorId = metricsWithState.get(0).getMonitorId();
    UUID taskId = metricsWithState.get(0).getTaskId();
    String compositeKey = metricsWithState.get(0).getCompositeKey();

    // retrieve only recent events for use in the final state evaluation
    List<SalusEnrichedMetric> contributingEvents = getRelevantEvents(metricsWithState);

    if (!isTaskWarm(contributingEvents)) {
      return;
    }

    String newState = determineNewEventState(contributingEvents);
    if (newState == null) {
      log.debug("Quorum was not met for task");
      return;
    }

    String oldState = getPreviousKnownState(tenantId, resourceId, monitorId, taskId);
    if (newState.equals(oldState)) {
      log.debug("Task={} state={} is unchanged", compositeKey, newState);
      return;
    }

    log.info("Task={} changed state from {} to {}", compositeKey, oldState, newState);
    handleStateChange(contributingEvents, newState, oldState);
  }

  private void handleStateChange(List<SalusEnrichedMetric> contributingEvents, String newState, String oldState) {
    // TODO: complete

    StateChange stateChange = generateStateChange(newState, oldState, contributingEvents);

    // add method that updates the previousState cache when this is saved
    stateChange = stateChangeRepository.save(stateChange);

    eventProducer.sendStateChange(stateChange);

    // send state change event to kafka
  }

  /**
   * Calculate the quorum state from a list of events.
   *
   * @param contributingEvents A list of events to be considered in the quorum calculation.
   * @return A state value if quorum is met, otherwise null.
   */
  private String determineNewEventState(List<SalusEnrichedMetric> contributingEvents) {
    int quorumCount =  (int) Math.floor((double) contributingEvents.size() / 2) + 1;

    return contributingEvents.stream()
        // convert list to a map of state counts
        .collect(Collectors.groupingBy(SalusEnrichedMetric::getState, Collectors.counting()))
        // then select any item that meets quorum
        .entrySet().stream().filter(count -> count.getValue() >= quorumCount)
        .findFirst().map(Entry::getKey).orElse(null /*TODO should this be an indeterminate status?*/);
  }

  /**
   * Returns a filtered list of events that are within the allowed time window.
   * Any "old" events will be disregarded as they are no longer relevant to the state change
   * calculations.
   * @param metrics
   * @return A filtered list of seen events.  At most one per zone.
   */
  private List<SalusEnrichedMetric> getRelevantEvents(List<SalusEnrichedMetric> metrics) {
    Instant window = getTimeWindowForStateCalculation(metrics);
    return metrics.stream()
        .filter(e -> isWithinTimeWindow(e, window))
        .collect(Collectors.toList());
  }

  /**
   * Determine if an event is within the required time window to be used as part of the
   * final alert state calculation.
   *
   * @param event A salus metric batch
   * @param window The oldest cutoff timestamp for events to be taken into consideration.
   * @return True if the event's timestamp is greater than the window, otherwise false.
   */
  private boolean isWithinTimeWindow(SalusEnrichedMetric event, Instant window) {
    return event.getStateEvaluationTimestamp().isAfter(window);
  }

  /**
   * Get the time period that can be used to determine which events should contribute towards
   * a state change calculation.
   *
   * This is based on the time of the latest event seen and the period of the corresponding monitor.
   *
   * @param metrics
   * @return
   */
  private Instant getTimeWindowForStateCalculation(List<SalusEnrichedMetric> metrics) {
    Instant newestDate = metrics.stream()
        .map(SalusEnrichedMetric::getStateEvaluationTimestamp)
        .max(Instant::compareTo)
        .orElseGet(Instant::now);

    Duration interval = getMonitorInterval(metrics.get(0).getTenantId(), metrics.get(0).getMonitorId());
    long windowSeconds = (long) (interval.toSeconds() * METRIC_WINDOW_MULTIPLIER);

    return newestDate.minusSeconds(windowSeconds);
  }

  @Cacheable(cacheNames = MONITOR_INTERVALS, key = "{#tenantId, #monitorId}")
  public Duration getMonitorInterval(String tenantId, UUID monitorId) {
    return monitorRepository.findByIdAndTenantId(monitorId, tenantId)
        .map(Monitor::getInterval)
        .orElse(DEFAULT_METRIC_WINDOW);
  }

  /**
   * Validates enough events have been received to confidently evaluate a valid state.
   * For remote monitor types this relates to the number of zones configured, for agent monitors
   * only a single event must be seen.
   *
   * This is required to handle new tasks that are still "warming up" and for any service restarts
   * where previous state has been lost.
   *
   * Once an event is classed as warm it will remain that way until a service restart occurs.
   * TODO: add this logic back in?  Track via a map in warmthTracker?  monitor/task updates should reset warmth?
   *
   * @param contributingEvents The "recent" observed events for each zone relating to this task event.
   * @return True if the enough events have been seen, otherwise false.
   */
  private boolean isTaskWarm(List<SalusEnrichedMetric> contributingEvents) {
    SalusEnrichedMetric metric = contributingEvents.get(0);

    int warmth;
    int expectedEventCount = getExpectedEventCountForMonitor(metric);

    if (contributingEvents.size() == expectedEventCount) {
      log.debug("Setting task={} as warm", metric);
      return true;
    } else if (contributingEvents.size() > expectedEventCount) {
      log.warn("Too many contributing events seen. "
              + "Monitor configured in {} zones but saw {} events for task={}",
          expectedEventCount, contributingEvents.size(), metric.getCompositeKey());
      eventProcessingAnomalies.tags(MetricTags.OPERATION_METRIC_TAG, "isTaskWarm",
          MetricTags.REASON, "more_events_than_zones")
          .register(meterRegistry).increment();
      return true;
    } else if ((warmth = taskWarmthTracker.getTaskWarmth(metric)) > expectedEventCount) {
      // This helps account for problematic zones without having to query / rely on their stored state.
      log.warn("Alarm warmth {} greater than configures zones. "
              + "Monitor configured in {} zones but saw {} events for task={}",
          warmth, expectedEventCount, contributingEvents.size(), metric.getCompositeKey());
      eventProcessingAnomalies.tags(MetricTags.OPERATION_METRIC_TAG, "isTaskWarm",
          MetricTags.REASON, "less_events_than_zones")
          .register(meterRegistry).increment();
      return true;
    } else {
      log.debug("New task is warming up. Monitor configured in {} zones but saw {} events for task={}",
          expectedEventCount, contributingEvents.size(), metric.getCompositeKey());
      return false;
    }
  }

  @Cacheable(cacheNames = EXPECTED_EVENT_COUNTS,
      key = "{#metric.tenantId, #metric.resourceId, #metric.monitorId}")
  public int getExpectedEventCountForMonitor(SalusEnrichedMetric metric) {
    return boundMonitorRepository.countAllByResourceIdAndMonitor_IdAndMonitor_TenantId(
        metric.getResourceId(),
        metric.getMonitorId(),
        metric.getTenantId());
  }

  /**
   * Retrieve the last stored state for a task.
   *
   * @return The previous state if one exists, otherwise null.
   */
  // @Cacheable
  // TODO add this
  private String getPreviousKnownState(String tenantId, String resourceId, UUID monitorId, UUID taskId) {
    return stateChangeRepository.findFirstByTenantIdAndResourceIdAndMonitorIdAndTaskId(
        tenantId, resourceId, monitorId, taskId)
        .map(StateChange::getState).orElse(null);
  }

  private StateChange generateStateChange(String state, String prevState, List<SalusEnrichedMetric> contributingEvents) {
    SalusEnrichedMetric event = contributingEvents.get(0);
    return new StateChange()
        .setTenantId(event.getTenantId())
        .setResourceId(event.getResourceId())
        .setMonitorId(event.getMonitorId())
        .setTaskId(event.getTaskId())
        .setState(state)
        .setPreviousState(prevState)
        .setMessage(null)
        .setEvaluationTimestamp(Instant.now())
        .setContributingEvents(contributingEvents.toString());
  }
}