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
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UniversalMetricHandler {
  private static final String SALUS_MONITORING_SYSTEM = "Salus";
  private static final String UNKNOWN_VALUE = "unknown";

  // deviceMetadata keys populated by umb-enrichment
  private static final String SALUS_DEVICE_ID_KEY = "deviceId";
  private static final String SALUS_DEVICE_NAME_KEY = "serverName";
  private static final String SALUS_DEVICE_DC_KEY = "datacenter";

  private static final String SALUS_RESOURCE_ID_KEY = "resourceId";
  private static final String SALUS_MONITOR_ID_KEY = "monitorId";
  private static final String SALUS_TASK_ID_KEY = "taskId";
  private static final String SALUS_ZONE_ID_KEY = "taskId";
  private static final String SALUS_ZONE_ID_DEFAULT = "agent";
  private static final String SALUS_MONITOR_TYPE = "monitorType";
  private static final String SALUS_MONITOR_SELECTOR_SCOPE = "selectorScope";

  EsperEngine esperEngine;

  @Autowired
  public UniversalMetricHandler(EsperEngine esperEngine) {
    this.esperEngine = esperEngine;
  }

  void processSalusMetricFrame(UniversalMetricFrame metric) throws IllegalArgumentException {

    if (metric.getMetricsCount() == 0) {
      throw new IllegalArgumentException(String.format("Empty metric frame received in %s", metric));
    }
//    will probably use monitorType instead of the measurement?
//    String measurement = metric.getMetrics(0).getGroup();
//    if (StringUtils.isBlank(measurement)) {
//      throw new IllegalArgumentException(String.format("Measurement is not set in %s", metric));
//    }

    String tenantId = metric.getTenantId();
    String accountType = metric.getAccountType().toString();
    String deviceId = metric.getDeviceMetadataOrDefault(SALUS_DEVICE_ID_KEY, UNKNOWN_VALUE);
    String deviceName = metric.getDeviceMetadataOrDefault(SALUS_DEVICE_NAME_KEY, UNKNOWN_VALUE);
    String deviceDc = metric.getDeviceMetadataOrDefault(SALUS_DEVICE_DC_KEY, UNKNOWN_VALUE);

    String resourceId = metric.getSystemMetadataOrThrow(SALUS_RESOURCE_ID_KEY);
    UUID monitorId = UUID.fromString(
        metric.getSystemMetadataOrThrow(SALUS_MONITOR_ID_KEY));
    // default to agent "zone" if no public zone is set
    String zoneId = metric.getSystemMetadataOrDefault(SALUS_ZONE_ID_KEY, SALUS_ZONE_ID_DEFAULT); // TODO: confirm zoneId is null for agents
    String monitorType = metric.getSystemMetadataOrThrow(SALUS_MONITOR_TYPE); // TODO: might need to get this from the metric group instead of system metadata?  Potential that we will receive lists of different metric types in future
    String monitorSelectorScope = metric.getSystemMetadataOrThrow(SALUS_MONITOR_SELECTOR_SCOPE);

    SalusEnrichedMetric salusMetric = (SalusEnrichedMetric) new SalusEnrichedMetric()
        .setResourceId(resourceId)
        .setMonitorId(monitorId)
        .setZoneId(zoneId)
        .setMonitorType(monitorType)
        .setMonitorSelectorScope(monitorSelectorScope)
        .setMonitoringSystem(SALUS_MONITORING_SYSTEM)
        .setTenantId(tenantId)
        .setAccountType(accountType)
        .setMetrics(metric.getMetricsList());

    sendMetricToEsper(salusMetric);
  }

  void sendMetricToEsper(SalusEnrichedMetric metric) {
    esperEngine.sendMetric(metric);
  }
}
