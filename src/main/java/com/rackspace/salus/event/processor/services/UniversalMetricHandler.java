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

import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.monplat.protocol.UniversalMetricFrame.MonitoringSystem;
import com.rackspace.salus.event.processor.engine.EsperEngine;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.subtype.SalusEventEngineTask;
import com.rackspace.salus.telemetry.repositories.SalusEventEngineTaskRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UniversalMetricHandler {
  private static final String SALUS_MONITORING_SYSTEM = MonitoringSystem.SALUS.toString();
  private static final String UNKNOWN_VALUE = "unknown";

  // deviceMetadata keys populated by umb-enrichment
  private static final String SALUS_DEVICE_ID_KEY = "device_id";
  private static final String SALUS_DEVICE_NAME_KEY = "server_name";
  private static final String SALUS_DEVICE_DC_KEY = "datacenter";

  private static final String SALUS_RESOURCE_ID_KEY = "resource_id";
  private static final String SALUS_MONITOR_ID_KEY = "monitor_id";
  private static final String SALUS_ZONE_ID_KEY = "monitoring_zone_id";
  private static final String SALUS_ZONE_ID_DEFAULT = "agent";
  private static final String SALUS_MONITOR_TYPE = "monitor_type";
  private static final String SALUS_MONITOR_SELECTOR_SCOPE = "monitor_selector_scope";

  private final EsperEngine esperEngine;
  private final SalusEventEngineTaskRepository salusTaskRepository;

  @Autowired
  public UniversalMetricHandler(EsperEngine esperEngine,
      SalusEventEngineTaskRepository salusTaskRepository) {
    this.esperEngine = esperEngine;
    this.salusTaskRepository = salusTaskRepository;
  }

  void processSalusMetricFrame(UniversalMetricFrame metric) throws IllegalArgumentException {
    SalusEnrichedMetric salusMetric = convertUniversalMetricToSalusMetric(metric);
    sendMetricToEsper(salusMetric);
  }

  void sendMetricToEsper(SalusEnrichedMetric metric) {
    esperEngine.sendMetric(metric);
  }

  SalusEnrichedMetric convertUniversalMetricToSalusMetric(UniversalMetricFrame universalMetric) {
    if (universalMetric.getMetricsCount() == 0) {
      throw new IllegalArgumentException(String.format("Empty metric frame received in %s", universalMetric));
    }
    String tenantId = universalMetric.getTenantId();
    String accountType = universalMetric.getAccountType().toString();

    // TODO : utilize these in the metric payload once umb enrichment is populating the values
    String deviceId = universalMetric.getDeviceMetadataOrDefault(SALUS_DEVICE_ID_KEY, UNKNOWN_VALUE);
    String deviceName = universalMetric.getDeviceMetadataOrDefault(SALUS_DEVICE_NAME_KEY, UNKNOWN_VALUE);
    String deviceDc = universalMetric.getDeviceMetadataOrDefault(SALUS_DEVICE_DC_KEY, UNKNOWN_VALUE);

    String resourceId = universalMetric.getSystemMetadataOrThrow(SALUS_RESOURCE_ID_KEY);
    UUID monitorId = UUID.fromString(
        universalMetric.getSystemMetadataOrThrow(SALUS_MONITOR_ID_KEY));
    // default to agent "zone" if no public zone is set
    String zoneId = universalMetric.getSystemMetadataOrDefault(SALUS_ZONE_ID_KEY, SALUS_ZONE_ID_DEFAULT); // TODO: confirm zoneId is null for agents
    String monitorType = universalMetric.getSystemMetadataOrThrow(SALUS_MONITOR_TYPE); // TODO: might need to get this from the metric group instead of system metadata?  Potential that we will receive lists of different metric types in future
    String monitorSelectorScope = universalMetric.getSystemMetadataOrThrow(SALUS_MONITOR_SELECTOR_SCOPE);

    return (SalusEnrichedMetric) new SalusEnrichedMetric()
        .setResourceId(resourceId)
        .setMonitorId(monitorId)
        .setZoneId(zoneId)
        .setMonitorType(monitorType)
        .setMonitorSelectorScope(monitorSelectorScope)
        .setMonitoringSystem(SALUS_MONITORING_SYSTEM)
        .setTenantId(tenantId)
        .setAccountType(accountType)
        .setMetrics(universalMetric.getMetricsList());
  }

  /**
   * Undeploys all tasks from esper related to the provided partitions.
   *
   * @param removedPartitions The partitions whose alarms will be undeployed.
   */
  public void removeTasksForPartitions(Set<Integer> removedPartitions) {
    log.info("Removing tasks from engine for {} partitions", removedPartitions.size());
    // load from db and then undeploy
    List<EventEngineTask> tasksToUndeploy = new ArrayList<>();
    for (Integer partition : removedPartitions) {
      List<SalusEventEngineTask> tasks = salusTaskRepository.findByPartition(partition);
      tasksToUndeploy.addAll(tasks);
    }
    log.info("TODO Remove tasks from {}", tasksToUndeploy);
  }

  /**
   * Deploys all tasks to esper related to the provided partitions.
   *
   * @param addedPartitions The partitions whose alarms will be deployed.
   */
  public void deployTasksForPartitions(Set<Integer> addedPartitions) {
    log.info("Adding tasks to engine for {} partitions", addedPartitions.size());
    // load from db and then deploy
    List<EventEngineTask> tasksToDeploy = new ArrayList<>();
    for (Integer partition : addedPartitions) {
      List<SalusEventEngineTask> tasks = salusTaskRepository.findByPartition(partition);
      tasksToDeploy.addAll(tasks);
    }
    log.info("TODO deploy tasks from {}", tasksToDeploy);
  }
}