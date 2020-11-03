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
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.Comparator;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.ComparisonExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression.Operator;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.StateExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.TaskState;
import com.rackspace.salus.telemetry.entities.subtype.SalusEventEngineTask;
import com.rackspace.salus.telemetry.model.MetricExpressionBase;
import com.rackspace.salus.telemetry.model.PercentageEvalNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@Ignore
public class StateEvaluatorTest {

  // default value for integer metrics
  static int VALUE = 5;

  List<ComparisonExpression> trueList = List.of(
      genCompExpression("total_cpu", Comparator.LESS_THAN, VALUE + 1),
      genCompExpression("total_cpu", Comparator.LESS_THAN_OR_EQUAL_TO, VALUE),
      genCompExpression("total_cpu", Comparator.GREATER_THAN, VALUE - 1),
      genCompExpression("total_cpu", Comparator.GREATER_THAN_OR_EQUAL_TO, VALUE),
      genCompExpression("total_cpu", Comparator.EQUAL_TO, VALUE),
      genCompExpression("total_cpu", Comparator.NOT_EQUAL_TO, VALUE - 1));

  List<ComparisonExpression> falseList = List.of(
      genCompExpression("total_cpu", Comparator.LESS_THAN, VALUE),
      genCompExpression("total_cpu", Comparator.LESS_THAN_OR_EQUAL_TO, VALUE - 1),
      genCompExpression("total_cpu", Comparator.GREATER_THAN, VALUE),
      genCompExpression("total_cpu", Comparator.GREATER_THAN_OR_EQUAL_TO, VALUE + 1),
      genCompExpression("total_cpu", Comparator.EQUAL_TO, VALUE - 1),
      genCompExpression("total_cpu", Comparator.NOT_EQUAL_TO, VALUE));

  List<ComparisonExpression> trueStringList = List.of(
      genCompExpression("banner", Comparator.REGEX_MATCH, "a.*z"),
      genCompExpression("banner", Comparator.NOT_REGEX_MATCH, "a.*1"));

  List<ComparisonExpression> falseStringList = List.of(
      genCompExpression("banner", Comparator.REGEX_MATCH, "a.*1"),
      genCompExpression("banner", Comparator.NOT_REGEX_MATCH, "a.*z"));

  private ComparisonExpression genCompExpression(String valueName, Comparator comparator,
      Object comparisonValue) {
    return new ComparisonExpression()
      .setComparator(comparator)
      .setValueName(valueName)
      .setComparisonValue(comparisonValue);
  }

  @Test
  public void criticalTest() {
    // critical is true for each
    for (ComparisonExpression comparisonExpression : trueList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getSalusEnrichedMetric();
      SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
    }
  }

  @Test
  public void okTest() {
    // critical is false for each
    for (ComparisonExpression comparisonExpression : falseList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getSalusEnrichedMetric();
      SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("OK");
    }
  }

  @Test
  public void criticalStringTest() {
    // critical is true for each
    for (ComparisonExpression comparisonExpression : trueStringList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getStringMetric("banner", "abcdz");
      SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
    }
  }

  @Test
  public void okStringTest() {
    // critical is false for each
    for (ComparisonExpression comparisonExpression : falseStringList) {
      String taskId = setTaskData(comparisonExpression);
      SalusEnrichedMetric s = getStringMetric("banner", "abcdz");
      SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
      assertThat(generatedMetric.getState()).isEqualTo("OK");
    }
  }

  @Test
  public void andTest() {
    // true AND true is critical
    String taskId = setLogicalTaskData(Operator.AND, trueList);
    SalusEnrichedMetric s = getSalusEnrichedMetric();
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // true AND false is ok state
    taskId = setLogicalTaskData(Operator.AND, List.of(trueList.get(0), falseList.get(0)));
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");

    // false AND false is ok state
    taskId = setLogicalTaskData(Operator.AND, falseList);
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");
  }

  @Test
  public void orTest() {
    // true OR true is critical
    String taskId = setLogicalTaskData(Operator.OR, trueList);
    SalusEnrichedMetric s = getSalusEnrichedMetric();
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // true OR false is critical
    taskId = setLogicalTaskData(Operator.OR, List.of(trueList.get(0), falseList.get(0)));
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");

    // false OR false is ok state
    taskId = setLogicalTaskData(Operator.OR, falseList);
    s = getSalusEnrichedMetric();
    generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    assertThat(generatedMetric.getState()).isEqualTo("OK");
  }

  @Test
  public void stringExpressionWithIntegerMetric() {
    ComparisonExpression comparisonExpression = trueStringList.get(0);
    String taskId = setTaskData(comparisonExpression, TaskState.OK);
    SalusEnrichedMetric s = getSalusEnrichedMetric("banner", 3);
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    // Note that even though there is no critical expression it still returns
    //  critical because of the bad parameter
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }      

  @Test
  public void stringExpressionWithIntegerExpressionValue() {
    ComparisonExpression comparisonExpression =
      genCompExpression("banner", Comparator.REGEX_MATCH, 1);
    String taskId = setTaskData(comparisonExpression, TaskState.OK);
    SalusEnrichedMetric s = getStringMetric("banner", "valid string");
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    // Note that even though there is no critical expression it still returns
    //  critical because of the bad parameter
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }
  
  @Test
  public void integerExpressionWithStringMetric() {
    ComparisonExpression comparisonExpression = trueStringList.get(0);
    String taskId = setTaskData(comparisonExpression, TaskState.OK);
    SalusEnrichedMetric s = getStringMetric("total_cpu", "bad integer parameter");
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    // Note that even though there is no critical expression it still returns
    //  critical because of the bad parameter
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }

  @Test
  public void integerExpressionWithStringExpressionValue() {
    ComparisonExpression comparisonExpression =
      genCompExpression("total_cpu", Comparator.LESS_THAN, "bad integer value");
    String taskId = setTaskData(comparisonExpression, TaskState.OK);
    SalusEnrichedMetric s = getSalusEnrichedMetric("total_cpu", 3);
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, taskId);
    // Note that even though there is no critical expression it still returns
    //  critical because of the bad parameter
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }

  @Test
  public void percentageTest() {
    // Test good parameter
    percentageTest(30, TaskState.CRITICAL);
    // Test bad parameter passing in a string instead of an integer
    percentageTest("bad integer parameter", TaskState.OK);
  }

  public void percentageTest(Object comparisonValue, TaskState state) {
    ComparisonExpression comparisonExpression =
      genCompExpression("percentage", Comparator.LESS_THAN, comparisonValue);
    PercentageEvalNode node = new PercentageEvalNode().setPart("part").setTotal("total");
    node.setAs("percentage");
    List<MetricExpressionBase> list = new ArrayList<>();
    list.add(node);
    String t = setTaskData(comparisonExpression, state, list);
    SalusEnrichedMetric s = getSalusEnrichedMetric("part", 10);
    Metric total = Metric.newBuilder().setName("total").setInt(100).build();
    s.getMetrics().add(total);
    SalusEnrichedMetric generatedMetric = StateEvaluator.evalMetricState(s, s, t);
    assertThat(generatedMetric.getState()).isEqualTo("CRITICAL");
  }

  private SalusEnrichedMetric getSalusEnrichedMetric() {
    return getSalusEnrichedMetric("total_cpu", VALUE);
  }

  private SalusEnrichedMetric getSalusEnrichedMetric(String name, int val) {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    Metric m = Metric.newBuilder().setName(name).setInt(val).setTimestamp(timestamp).build();
    List<Metric> list = new ArrayList<>();
    list.add(m);
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
    return setTaskData(comparisonExpression, TaskState.CRITICAL);
  }

  private String setTaskData(ComparisonExpression comparisonExpression,
      TaskState state) {
    return setTaskData(comparisonExpression, state, null);
  }

  private String setTaskData(ComparisonExpression comparisonExpression,
      TaskState state, List<MetricExpressionBase> customMetrics) {
    String taskId = UUID.randomUUID().toString();

    StateExpression expression = new StateExpression()
        .setExpression(comparisonExpression)
        .setState(state);
    EventEngineTaskParameters parameters =
        new EventEngineTaskParameters().setStateExpressions(List.of(expression));
    if (customMetrics != null) {
      parameters.setCustomMetrics(customMetrics);
    }
    SalusEventEngineTask task = new SalusEventEngineTask();
    task.setTaskParameters(parameters);
    StateEvaluator.saveTaskData(taskId, "deploymentId", task);
    return taskId;
  }


  private String setLogicalTaskData(Operator operator,
      List<ComparisonExpression> comparisonExpressions) {
    String taskId = UUID.randomUUID().toString();
    LogicalExpression logicalExpression = new LogicalExpression()
        .setOperator(operator);
    //noinspection unchecked
    logicalExpression.setExpressions((List) comparisonExpressions);
    StateExpression expression = new StateExpression()
        .setExpression(logicalExpression)
        .setState(TaskState.CRITICAL);
    EventEngineTaskParameters parameters = new EventEngineTaskParameters()
        .setStateExpressions(List.of(expression));
    SalusEventEngineTask task = new SalusEventEngineTask();
    task.setTaskParameters(parameters);
    StateEvaluator.saveTaskData(taskId, "deploymentId", task);
    return taskId;
  }
}
