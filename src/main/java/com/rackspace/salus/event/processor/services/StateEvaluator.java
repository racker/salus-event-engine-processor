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

import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateEvaluator {

  /**
   * This method will be used within the Esper query that is added to the engine for each
   * task loaded.
   *
   * @param metric
   * @param taskId
   * @return
   */
  public static SalusEnrichedMetric generateEnrichedMetric(
      SalusEnrichedMetric metric, String taskId) {
    log.info("gbj was here: " + metric.getResourceId());
    return metric
        .setState("TODO fix this state")
        .setTaskId(UUID.fromString(taskId))
        // TODO use metric timestamps instead of `now`` ?
        .setStateEvaluationTimestamp(Instant.now());
  }

}
