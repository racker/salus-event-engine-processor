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

import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import java.time.Instant;
import java.util.Collections;
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
        .setMonitoringSystem("Salus")
        .setTenantId(tenantId)
        .setAccountType(accountType)
        .setMetrics(Collections.emptyList())
        .setTags(Map.of(
            "os", "linux",
            "dc", "private"
        ));
  }
}
