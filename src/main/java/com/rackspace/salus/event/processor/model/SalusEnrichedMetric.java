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

package com.rackspace.salus.event.processor.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

@Data
public class SalusEnrichedMetric extends EnrichedMetric {
  // initial fields are set when ingesting a new metric
  String resourceId;
  UUID monitorId;
  String zoneId;
  String monitorType;
  String monitorSelectorScope;
  // the below fields are set when the metric matches a Task Query within Esper
  UUID taskId;
  List<String> excludedResourceIds;
  Instant stateEvaluationTimestamp;
  Map<String, Integer> expectedStateCounts;
//  EventState state;
  String state;

  public String getCompositeKey() {
    return String.join(":",
        tenantId, resourceId, monitorId.toString(), taskId.toString());
  }
}
