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

package com.rackspace.salus.event.processor.utils;

import static java.util.Collections.singletonMap;

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.monplat.protocol.UniversalMetricFrame.MonitoringSystem;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.model.ConfigSelectorScope;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.RandomStringUtils;

public class TestDataGenerators {

  public static SalusEnrichedMetric createSalusEnrichedMetric() {
    String resourceId = RandomStringUtils.randomAlphabetic(5);
    UUID monitorId = UUID.randomUUID();
    UUID taskId = UUID.randomUUID();
    String zoneId = RandomStringUtils.randomAlphabetic(5);
    String monitorType = RandomStringUtils.randomAlphabetic(5);
    String monitorSelectorScope = RandomStringUtils.randomAlphabetic(5);
    String tenantId = RandomStringUtils.randomAlphabetic(5);
    String accountType = RandomStringUtils.randomAlphabetic(5);

    return (SalusEnrichedMetric) new SalusEnrichedMetric()
        .setResourceId(resourceId)
        .setMonitorId(monitorId)
        .setTaskId(taskId)
        .setZoneId(zoneId)
        .setMonitorType(monitorType)
        .setState("original")
        .setStateEvaluationTimestamp(Instant.now())
        .setExpectedStateCounts(Map.of(
            "one", 1,
            "two", 2,
            "three", 3,
            "original", 2
        ))
        .setMonitorSelectorScope(monitorSelectorScope)
        .setMonitoringSystem(MonitoringSystem.SALUS.toString())
        .setTenantId(tenantId)
        .setAccountType(accountType)
        .setMetrics(Collections.emptyList())
        .setTags(Map.of(
            "os", "linux",
            "dc", "private"
        ));
  }

  public static UniversalMetricFrame generateUniversalMetricFrame() {
    final Timestamp timestamp = Timestamp.newBuilder()
        .setSeconds(1598458510).build();

    List<Metric> metricList = new ArrayList<>();
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("fvalue")
        .setFloat(3.14)
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("svalue")
        .setString("testing")
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("ivalue")
        .setInt(5)
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());

    Map<String, String> deviceMetadata = Map.of(
        "deviceId", "myDeviceId",
        "deviceName", "myDeviceName",
        "datacenter", "dc1"
    );

    Map<String, String> systemMetadata = Map.of(
        "resourceId", "resource-1",
        "monitorId", "00000000-0000-0000-0000-000000000001",
        "zoneId", "zone-1",
        "monitorType", "ping",
        "selectorScope", ConfigSelectorScope.REMOTE.toString()
    );

    return UniversalMetricFrame.newBuilder()
        .setTenantId(RandomStringUtils.randomAlphanumeric(5))
        .setAccountType(UniversalMetricFrame.AccountType.MANAGED_HOSTING)
        .setMonitoringSystem(UniversalMetricFrame.MonitoringSystem.SALUS)
        .putAllSystemMetadata(systemMetadata)
        .putAllDeviceMetadata(deviceMetadata)
        .setDevice("d-1")
        .addAllMetrics(metricList)
        .build();
  }
}
