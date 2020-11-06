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

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.monplat.protocol.Metric.ValueCase;
import com.rackspace.salus.event.processor.model.EsperTaskData;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.Comparator;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.ComparisonExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.Expression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.LogicalExpression.Operator;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.StateExpression;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.TaskState;
import com.rackspace.salus.telemetry.model.MetricExpressionBase;
import com.rackspace.salus.telemetry.model.PercentageEvalNode;
import com.rackspace.salus.telemetry.model.DerivativeNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateEvaluator {

  private static final Map<String, EsperTaskData> taskDataMap = new ConcurrentHashMap<>();
  private static final Map<Comparator, BiFunction<Double, Double, Boolean>> numericCompareFunctions =
      Map.of(Comparator.EQUAL_TO, Double::equals,
             Comparator.NOT_EQUAL_TO, (op1, op2)->(!op1.equals(op2)),
             Comparator.LESS_THAN, (op1, op2)->(op1 < op2),
             Comparator.LESS_THAN_OR_EQUAL_TO, (op1, op2)->(op1 <= op2),
             Comparator.GREATER_THAN, (op1, op2)->(op1 > op2),
             Comparator.GREATER_THAN_OR_EQUAL_TO, (op1, op2)->(op1 >= op2));

  private static Boolean matchRegex(String s, String pattern) {
    Pattern p = Pattern.compile(pattern);
    Matcher m = p.matcher(s);
    return m.find();
  }

  private static final Map<Comparator, BiFunction<String, String, Boolean>> stringCompareFunctions =
      Map.of(Comparator.REGEX_MATCH, StateEvaluator::matchRegex,
             Comparator.NOT_REGEX_MATCH, (op1, op2)->!StateEvaluator.matchRegex(op1, op2));

  public static void saveTaskData(String taskId, String deploymentId, EventEngineTask task) {
    taskDataMap.put(taskId, new EsperTaskData(deploymentId, task));
  }

  public static String removeTaskData(String taskId) {
    EsperTaskData taskData = taskDataMap.remove(taskId);
    if (taskData == null) {
      log.warn("Could not remove task data for task={}", taskId);
      return null;
    }
    return taskData.getDeploymentId();
  }

  private static SalusEnrichedMetric setMetricFields(SalusEnrichedMetric metric, String state,
      String taskId) {
    return metric
        .setState(state)
        .setTaskId(UUID.fromString(taskId))
        // TODO use metric timestamps instead of `now`` ?
        .setStateEvaluationTimestamp(Instant.now());
  }

  public static SalusEnrichedMetric evalMetricState(SalusEnrichedMetric metric, SalusEnrichedMetric prevMetric, String taskId) {
    EsperTaskData data = taskDataMap.get(taskId);

    //prep custom metrics
    List<Metric> metricList = metric.getMetrics(), prevMetricList = null;
    if (prevMetric != null) {
      prevMetricList = prevMetric.getMetrics();
    }

    // Todo: send these to UMB
    List<Metric> evaluatedCustomMetricList = new ArrayList<>();

    List<MetricExpressionBase> customMetrics =
        data.getEventEngineTask().getTaskParameters().getCustomMetrics();
    if (customMetrics != null) {
      for (MetricExpressionBase customMetric : customMetrics) {
        try {
          evaluatedCustomMetricList.add(evalCustomMetric(customMetric, metricList, prevMetricList));
        } catch (IllegalArgumentException e) {
          log.warn("Bad metric parameter used: {}", e.getMessage());
          return setMetricFields(metric, "CRITICAL", taskId);
        }
      }
    }
    if (evaluatedCustomMetricList.size() > 0) {
      metricList.addAll(evaluatedCustomMetricList);
    }
    // sort state expressions
    Map<TaskState, StateExpression> stateExpressions = new HashMap<>();
    for (StateExpression stateExpression : data.getEventEngineTask().getTaskParameters()
        .getStateExpressions()) {
      stateExpressions.put(stateExpression.getState(), stateExpression);
    }

    // Check each type of stateExpression and return the first one that is true
    for (String state : List.of("CRITICAL", "WARNING", "OK")) {
      StateExpression current = stateExpressions.get(TaskState.valueOf(state));
      try {
        if (current != null && getExpressionValue(current, metric)) {
          return setMetricFields(metric, state, taskId);
        }
      } catch (IllegalArgumentException e) {
        log.warn("Bad metric parameter used: {}", e.getMessage());
        return setMetricFields(metric, "CRITICAL", taskId);
      }
    }
    // None were true so return ok
    return setMetricFields(metric, "OK", taskId);
  }

  private static boolean getExpressionValue(StateExpression current,
      SalusEnrichedMetric metric) {
    return getExpressionValue(current.getExpression(), metric);
  }

  private static boolean getExpressionValue(Expression current,
      SalusEnrichedMetric metric) {
    if (current instanceof LogicalExpression) {
      return getLogicalValue((LogicalExpression) current, metric);
    } else {
      return getComparisonValue((ComparisonExpression) current, metric);
    }
  }

  private static boolean getLogicalValue(LogicalExpression expression,
      SalusEnrichedMetric metric) {
    // With Operator.AND return as soon as false is found
    if (expression.getOperator() == Operator.AND) {
      for (Expression subExpression : expression.getExpressions()) {
        if (!getExpressionValue(subExpression, metric))
          return false;
      }
      return true;  // Operator.AND is true if all are true
    } else {      // With Operator.OR, return as soon as true is found
      for (Expression subExpression : expression.getExpressions()) {
        if (getExpressionValue(subExpression, metric))
          return true;
      }
      return false; // Operator.OR is false if all are false
    }
  }

  private static boolean getComparisonValue(ComparisonExpression expression,
      SalusEnrichedMetric metric) {
    if (stringCompareFunctions.containsKey(expression.getComparator())) {
      return getStringValue(expression, metric);
    } else {
      return getNumericValue(expression, metric);
    }
  }

  private static boolean getNumericValue(ComparisonExpression expression,
      SalusEnrichedMetric metric) {
    Double operand1 = null, operand2;
    // Find first operand in metrics and convert to double
    for (Metric m : metric.getMetrics()) {
      operand1 = getNumericOperandFromMetric(m, expression.getValueName());
      if (operand1 != null)
        break;
    }
    if (operand1 == null) {
      throw new IllegalArgumentException("missing/incorrect type for operand in Metrics: "
          + expression.getValueName());
    }

    // Find second operand from comparisionValue and convert to double
    operand2 = getNumericOperandFromExpression(expression);
    return numericCompareFunctions.get(expression.getComparator()).apply(operand1, operand2);
  }

  private static Double getNumericOperandFromMetric(Metric m, String name) {
    if (m.getName().equals(name)) {
      if (m.getValueCase() == ValueCase.INT) {
        return (double) m.getInt();
      } else if (m.getValueCase() == ValueCase.FLOAT){
        return m.getFloat();
      }
    }
    return null;
  }

  private static Double getNumericOperandFromExpression(ComparisonExpression expression) {
    if (expression.getComparisonValue() instanceof Integer) {
      return ((Integer) expression.getComparisonValue()).doubleValue();
    } else if (expression.getComparisonValue() instanceof Double) {
      return (Double)expression.getComparisonValue();
    } else {
      throw new IllegalArgumentException("Illegal comparison object for: " +
          expression.getValueName());
    }
  }
  
  private static boolean getStringValue(ComparisonExpression expression,
      SalusEnrichedMetric metric) {
    // Find first operand in metrics and make sure it is string
    String operand1 = null, operand2;
    for (Metric m : metric.getMetrics()) {
      operand1 = getStringOperandFromMetric(m, expression.getValueName());
      if (operand1 != null)
        break;
    }
    if (operand1 == null) {
      throw new IllegalArgumentException("missing/incorrect type for operand in Metrics: " +
          expression.getValueName());
    }

    // Find second operand from comparisionValue and make sure it is string
    operand2 = getStringOperandFromExpression(expression);
    return stringCompareFunctions.get(expression.getComparator()).apply(operand1, operand2);
  }

  private static String getStringOperandFromMetric(Metric m, String name) {
    if (m.getName().equals(name)) {
      if (m.getValueCase() ==  ValueCase.STRING) {
        return m.getString();
      }
    }
    return null;
  }

  private static String getStringOperandFromExpression(ComparisonExpression expression) {
    if (expression.getComparisonValue() instanceof String) {
      return (String) expression.getComparisonValue();
    } else {
      throw new IllegalArgumentException("Illegal comparison object for: " +
          expression.getValueName());
    }
  }

  // Generate the synthetic metrics, (currently just percent and rate)
  static private Metric evalCustomMetric(MetricExpressionBase node, List<Metric>metrics, List<Metric>prevMetrics) {
    if (node instanceof PercentageEvalNode) {
      return evalCustomMetric((PercentageEvalNode)node, metrics);
    } else if (node instanceof DerivativeNode) {
      return evalCustomMetric((DerivativeNode)node, metrics, prevMetrics);
    } else {
      throw new IllegalArgumentException("percent and rate are the only custom metrics currently supported.");
    }
  }

  static private Metric evalCustomMetric(PercentageEvalNode percentageEvalNode, List<Metric>metrics) {
    Double part = null, total = null;
    double percentage;
    Timestamp timestamp = null;
    // get the partial value
    for (Metric metric : metrics) {
      if (part == null) {
        part = getNumericOperandFromMetric(metric, percentageEvalNode.getPart());
        if (part != null)
          timestamp = metric.getTimestamp();
      }
      if (total == null)
        total = getNumericOperandFromMetric(metric, percentageEvalNode.getTotal());

      // calculate the percent
      if (part != null && total != null) {
        percentage = part / total * 100;
        return Metric
                .newBuilder()
                .setName(percentageEvalNode.getAs())
                .setFloat(percentage)
                .setTimestamp(timestamp).build();
      }
    }
    throw new IllegalArgumentException("percentage part/total not found in metric.");
  }

  static private Metric evalCustomMetric(DerivativeNode node, List<Metric>metrics, List<Metric>prevMetrics) {
    Timestamp timestamp = null, oldTimestamp = null;
    Double metricValue = null, oldMetricValue = null;
    // get current metric value
    for (Metric metric : metrics) {
      metricValue = getNumericOperandFromMetric(metric, node.getMetric());
      if (metricValue != null) {
        timestamp = metric.getTimestamp();
        break;
      }
    }
    // get previous metric value
    for (Metric metric : prevMetrics) {
      oldMetricValue = getNumericOperandFromMetric(metric, node.getMetric());
      if (oldMetricValue != null) {
        oldTimestamp = metric.getTimestamp();
        break;
      }
    }
    // calculate the rate
    if (metricValue != null && oldMetricValue != null) {
      // get rate per second
      double rate = (metricValue - oldMetricValue)/(timestamp.getSeconds() - oldTimestamp.getSeconds());
      // convert to rate per duration
      rate = rate * node.getDuration().getSeconds();
      return Metric
              .newBuilder()
              .setName(node.getAs())
              .setFloat(rate)
              .setTimestamp(timestamp).build();
    } else {
      throw new IllegalArgumentException("Metric not found: " + node.getMetric());
    }
  }

  // Returns true if the previous metric is required to evaluate custom metrics
  public static boolean includePrev(EventEngineTask task) {
    List<MetricExpressionBase> customMetrics = task.getTaskParameters().getCustomMetrics();
    if (customMetrics == null) {
      return false;
    }
    for (MetricExpressionBase customMetric : customMetrics) {
      if (customMetric instanceof DerivativeNode ) {
        return true;
      }
    }
    return false;
  }
}
