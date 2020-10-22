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

import com.rackspace.monplat.protocol.Metric;
import com.rackspace.salus.event.processor.model.EsperTaskData;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.model.MetricExpressionBase;
import com.rackspace.salus.telemetry.model.PercentageEvalNode;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateEvaluator {

  private static Map<String, EsperTaskData> taskDataMap = new HashMap<>();

  public static void saveTaskData(String taskId, String deploymentId, EventEngineTask task) {
    taskDataMap.put(taskId, new EsperTaskData(deploymentId, task));
  }

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
    EsperTaskData data = taskDataMap.get(taskId);
    List<MetricExpressionBase> customMetrics = data.getEventEngineTask().getTaskParameters().getCustomMetrics();
    StateEvaluator.MetricEvaluator evaluator = new StateEvaluator.MetricEvaluator(metric.getMetrics());
    customMetrics.stream().map(m -> evaluator.eval(m));

    return metric
        .setState("TODO fix this state")
        .setTaskId(UUID.fromString(taskId))
        // TODO use metric timestamps instead of `now`` ?
        .setStateEvaluationTimestamp(Instant.now());
  }
  @Data
  static class MetricEvaluator {
    @NonNull
    private List<Metric> metrics;
    Map.Entry<String, Double> eval(MetricExpressionBase node) {
      return new AbstractMap.SimpleEntry<String, Double>(((PercentageEvalNode)node).getPart(), 1.0);
    }
  }
}
