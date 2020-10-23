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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.Comparator;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.ComparisonExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.StateExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.TaskState;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class StateEvaluatorTest {

  @Before
  public void setup() {
  }

  @Test
  public void mainTest() {

    String taskId = UUID.randomUUID().toString();
    ComparisonExpression c = new ComparisonExpression()
        .setComparator(Comparator.LESS_THAN)
        .setValueName("total_cpu")
        .setComparisonValue(3);
    StateExpression expression = new StateExpression()
        .setExpression(c)
        .setState(TaskState.CRITICAL);
    EventEngineTaskParameters parameters = new EventEngineTaskParameters().setStateExpressions(List.of(expression));
    EventEngineTask task = new EventEngineTask().setTaskParameters(parameters);
    StateEvaluator.saveTaskData(taskId, "deploymentId", task);

    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    Metric part = Metric.newBuilder().setName("part").setInt(2).setTimestamp(timestamp).build();
    Metric total = Metric.newBuilder().setName("total").setInt(10).setTimestamp(timestamp).build();
    Metric totalCpu = Metric.newBuilder().setName("total_cpu").setInt(1).setTimestamp(timestamp).build();
    List<Metric> list = List.of(part, total, totalCpu);
    SalusEnrichedMetric s =  new SalusEnrichedMetric();
    s.setMetrics(list);
    SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }
}
