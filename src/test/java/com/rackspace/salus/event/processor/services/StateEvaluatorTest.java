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
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression.Operator;
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

  int threshold = 5;
  List<ComparisonExpression> trueList = List.of(
    new ComparisonExpression()
      .setComparator(Comparator.LESS_THAN)
      .setValueName("total_cpu")
      .setComparisonValue(threshold + 1 ),
    new ComparisonExpression()
      .setComparator(Comparator.LESS_THAN_OR_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold),
    new ComparisonExpression()
      .setComparator(Comparator.GREATER_THAN)
      .setValueName("total_cpu")
      .setComparisonValue(threshold - 1),
    new ComparisonExpression()
      .setComparator(Comparator.GREATER_THAN_OR_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold),
    new ComparisonExpression()
      .setComparator(Comparator.EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold),
    new ComparisonExpression()
      .setComparator(Comparator.NOT_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold - 1 ));

  List<ComparisonExpression> falseList = List.of(
    new ComparisonExpression()
      .setComparator(Comparator.LESS_THAN)
      .setValueName("total_cpu")
      .setComparisonValue(threshold),
    new ComparisonExpression()
      .setComparator(Comparator.LESS_THAN_OR_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold - 1),
    new ComparisonExpression()
      .setComparator(Comparator.GREATER_THAN)
      .setValueName("total_cpu")
      .setComparisonValue(threshold),
    new ComparisonExpression()
      .setComparator(Comparator.GREATER_THAN_OR_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold + 1),
    new ComparisonExpression()
      .setComparator(Comparator.EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold - 1),
    new ComparisonExpression()
      .setComparator(Comparator.NOT_EQUAL_TO)
      .setValueName("total_cpu")
      .setComparisonValue(threshold));

  List<ComparisonExpression> trueStringList = List.of(
    new ComparisonExpression()
      .setComparator(Comparator.REGEX_MATCH)
      .setValueName("banner")
      .setComparisonValue("a.*z"),
    new ComparisonExpression()
      .setComparator(Comparator.NOT_REGEX_MATCH)
      .setValueName("banner")
      .setComparisonValue("a.*1"));

  List<ComparisonExpression> falseStringList = List.of(
    new ComparisonExpression()
      .setComparator(Comparator.REGEX_MATCH)
      .setValueName("banner")
      .setComparisonValue("a.*1"),
    new ComparisonExpression()
      .setComparator(Comparator.NOT_REGEX_MATCH)
      .setValueName("banner")
      .setComparisonValue("a.*z"));



  @Test
  public void criticalTest() {
    // critical is true for each
    for (ComparisonExpression comparisonExpression : trueList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getSalusEnrichedMetric();
      SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
    }
  }


  @Test
  public void okTest() {
    // critical is false for each
    for (ComparisonExpression comparisonExpression : falseList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getSalusEnrichedMetric();
      SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("OK");
    }
  }

  @Test
  public void criticalStringTest() {
    // critical is true for each
    for (ComparisonExpression comparisonExpression : trueStringList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getStringMetric("banner", "abcdz");
      SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
    }
  }

  @Test
  public void okStringTest() {
    // critical is false for each
    for (ComparisonExpression comparisonExpression : falseStringList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getStringMetric("banner", "abcdz");
      SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("OK");
    }
  }

  @Test
  public void andTest() {
    // true AND true is critical
    String taskId = setLogicalTaskData(Operator.AND, trueList);
    SalusEnrichedMetric s = getSalusEnrichedMetric();
    SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // true AND false is ok
    taskId = setLogicalTaskData(Operator.AND, List.of(trueList.get(0), falseList.get(0)));
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");

    // fals AND false is ok
    taskId = setLogicalTaskData(Operator.AND, falseList);
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");
  }


  @Test
  public void orTest() {
    // true OR true is critical
    String taskId = setLogicalTaskData(Operator.OR, trueList);
    SalusEnrichedMetric s = getSalusEnrichedMetric();
    SalusEnrichedMetric generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // true OR false is critical
    taskId = setLogicalTaskData(Operator.OR, List.of(trueList.get(0), falseList.get(0)));
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // ok OR ok is ok
    taskId = setLogicalTaskData(Operator.OR, falseList);
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.generateEnrichedMetric(s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");
  }

  private SalusEnrichedMetric getSalusEnrichedMetric() {
    return getSalusEnrichedMetric("total_cpu", threshold);
  }

  private SalusEnrichedMetric getSalusEnrichedMetric(String name, int val) {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    Metric m = Metric.newBuilder().setName(name).setInt(val).setTimestamp(timestamp).build();
    List<Metric> list = List.of(m);
    SalusEnrichedMetric s =  new SalusEnrichedMetric();
    s.setMetrics(list);
    return s;
  }

  private SalusEnrichedMetric getStringMetric(String name, String val) {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    Metric m = Metric.newBuilder().setName(name).setString(val).setTimestamp(timestamp).build();
    List<Metric> list = List.of(m);
    SalusEnrichedMetric s =  new SalusEnrichedMetric();
    s.setMetrics(list);
    return s;
  }

  private String setTaskData(ComparisonExpression comparisonExpression) {
    String taskId = UUID.randomUUID().toString();

    StateExpression expression = new StateExpression()
        .setExpression(comparisonExpression)
        .setState(TaskState.CRITICAL);
    EventEngineTaskParameters parameters = new EventEngineTaskParameters().setStateExpressions(List.of(expression));
    EventEngineTask task = new EventEngineTask().setTaskParameters(parameters);
    StateEvaluator.saveTaskData(taskId, "deploymentId", task);
    return taskId;
  }

  private String setLogicalTaskData(Operator operator, List<ComparisonExpression> comparisonExpressions) {
    String taskId = UUID.randomUUID().toString();
    LogicalExpression logicalExpression = new LogicalExpression()
        .setOperator(operator)
        .setExpressions((List)comparisonExpressions);
    StateExpression expression = new StateExpression()
        .setExpression(logicalExpression)
        .setState(TaskState.CRITICAL);
    EventEngineTaskParameters parameters = new EventEngineTaskParameters()
        .setStateExpressions(List.of(expression));
    EventEngineTask task = new EventEngineTask().setTaskParameters(parameters);
    StateEvaluator.saveTaskData(taskId, "deploymentId", task);
    return taskId;
  }
}
