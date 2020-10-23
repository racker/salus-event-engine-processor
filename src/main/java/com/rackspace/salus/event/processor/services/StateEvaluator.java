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
import java.time.Instant;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateEvaluator {

  private static final Map<String, EsperTaskData> taskDataMap = new HashMap<>();
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
    return m.matches();
  }
  private static final Map<Comparator, BiFunction<String, String, Boolean>> stringCompareFunctions =
      Map.of(Comparator.REGEX_MATCH, StateEvaluator::matchRegex,
             Comparator.NOT_REGEX_MATCH, (op1, op2)->!StateEvaluator.matchRegex(op1, op2));

  public static void saveTaskData(String taskId, String deploymentId, EventEngineTask task) {
    taskDataMap.put(taskId, new EsperTaskData(deploymentId, task));
  }

  public static SalusEnrichedMetric initMetric(SalusEnrichedMetric metric, String state, String taskId) {
    return metric
        .setState(state)
        .setTaskId(UUID.fromString(taskId))
        // TODO use metric timestamps instead of `now`` ?
        .setStateEvaluationTimestamp(Instant.now());
  }
  public static SalusEnrichedMetric generateEnrichedMetric(
      SalusEnrichedMetric metric, String taskId) {
    log.info("gbj was here: " + metric.getResourceId());
    EsperTaskData data = taskDataMap.get(taskId);
    Map<String, Double> customMetricData = new HashMap<>();

    //prep custom metrics
    List<MetricExpressionBase> customMetrics = data.getEventEngineTask().getTaskParameters().getCustomMetrics();
    if (customMetrics != null) {
      StateEvaluator.MetricEvaluator evaluator = new StateEvaluator.MetricEvaluator(
          metric.getMetrics());
      for (MetricExpressionBase customMetric : customMetrics) {
        Map.Entry<String, Double> entry = evaluator.eval(customMetric);
        customMetricData.put(entry.getKey(), entry.getValue());
      }
    }
    // sort state expressions
    Map<TaskState, StateExpression> stateExpressions = new HashMap<>();
    for (StateExpression stateExpression : data.getEventEngineTask().getTaskParameters()
        .getStateExpressions()) {
      stateExpressions.put(stateExpression.getState(), stateExpression);
    }

    for (String state : List.of("CRITICAL", "WARNING", "OK")) {
      StateExpression current = stateExpressions.get(TaskState.valueOf(state));
      if (current != null && getStateExpressionValue(current, metric, customMetricData)) {
        log.info("gbj found state: " + state);
        return initMetric(metric, state, taskId );
      }
    }
    log.info("gbj set ok ");
    return initMetric(metric, "OK", taskId);
  }

  private static boolean getStateExpressionValue(StateExpression current, SalusEnrichedMetric metric, Map<String, Double>customMetricData) {
    if (current.getExpression() instanceof LogicalExpression) {
      return getLogicalValue((LogicalExpression) current.getExpression(), metric, customMetricData);
    } else {
      return getComparisonValue((ComparisonExpression) current.getExpression(), metric, customMetricData);
    }
  }

  private static boolean getLogicalValue(LogicalExpression expression, SalusEnrichedMetric metric, Map<String, Double>customMetricData) {
    boolean retval;
    if (expression.getOperator() == Operator.AND) {
      for (Expression subExpression : expression.getExpressions()) {
        if (subExpression instanceof LogicalExpression) {
          retval = getLogicalValue((LogicalExpression) subExpression, metric, customMetricData);

        } else {
          retval = getComparisonValue((ComparisonExpression) subExpression, metric, customMetricData);
        }
        if (!retval) {
          return false;
        }
      }
    return true;  // Operator.AND is true if all are true
    } else {  // Operator.OR
      for (Expression subExpression : expression.getExpressions()) {
        if (subExpression instanceof LogicalExpression) {
          retval = getLogicalValue((LogicalExpression) subExpression, metric, customMetricData);

        } else {
          retval = getComparisonValue((ComparisonExpression) subExpression, metric, customMetricData);

        }
        if (retval) {
          return true;
        }
      }
      return false; // Operator.OR is false if all are false
    }
  }

  private static boolean getComparisonValue(ComparisonExpression expression,
      SalusEnrichedMetric metric, Map<String, Double>customMetricData) {
    if (expression.getComparator() == Comparator.REGEX_MATCH ||
        expression.getComparator() == Comparator.NOT_REGEX_MATCH) {
      return getStringValue(expression, metric);
    } else {
      return getNumericValue(expression, metric, customMetricData);
    }
  }

  private static boolean getNumericValue(ComparisonExpression expression, SalusEnrichedMetric metric, Map<String, Double> customMetricData) {
    Double operand1 = null, operand2;
    for (Metric m : metric.getMetrics()) {
      if (m.getName().equals(expression.getValueName())) {
        if (m.getValueCase() == ValueCase.INT) {
          operand1 = (double) m.getInt();
        } else {
          operand1 = m.getFloat();
        }
      }
    }
    if (operand1 == null) {
      operand1 = customMetricData.get(expression.getValueName());
    }
    if (operand1 == null) {
      throw new IllegalArgumentException("missing/incorrect type for operand in Metrics: " + expression.getValueName());
    }

    if (expression.getComparisonValue() instanceof Integer) {
      operand2 = ((Integer) expression.getComparisonValue()).doubleValue();
    } else if (expression.getComparisonValue() instanceof Double) {
      operand2 = (Double)expression.getComparisonValue();
    } else {
      throw new IllegalArgumentException("Illegal comparison object for: " + expression.getValueName());
    }
    return numericCompareFunctions.get(expression.getComparator()).apply(operand1, operand2);
  }
  


  private static boolean getStringValue(ComparisonExpression expression, SalusEnrichedMetric metric) {
    String operand1 = null, operand2;
    for (Metric m : metric.getMetrics()) {
      if (m.getName().equals(expression.getValueName())) {
        if (m.getValueCase() ==  ValueCase.STRING) {
          operand1 = m.getString();
        } 
      }
    }
    if (operand1 == null) {
      throw new IllegalArgumentException("missing/incorrect type for operand in Metrics: " + expression.getValueName());
    }

    if (expression.getComparisonValue() instanceof String) {
      operand2 = (String)expression.getComparisonValue();
    } else {
      throw new IllegalArgumentException("Illegal comparison object for: " + expression.getValueName());
    }
    return stringCompareFunctions.get(expression.getComparator()).apply(operand1, operand2);
  }

  @Data
  static class MetricEvaluator {
    @NonNull
    private List<Metric> metrics;
    Map.Entry<String, Double> eval(MetricExpressionBase node) {
      Double part = null, total = null, percentage;
      PercentageEvalNode percentageEvalNode = (PercentageEvalNode)node;
      for (Metric metric : metrics) {
        if (metric.getName().equals(percentageEvalNode.getPart())) {
          if (metric.getValueCase() == ValueCase.INT) {
            part = (double) metric.getInt();
          } else {
            part = metric.getFloat();
          }
        }
        if (metric.getName().equals(percentageEvalNode.getTotal())) {
          if (metric.getValueCase() == ValueCase.INT) {
            total = (double) metric.getInt();
          } else {
            total = metric.getFloat();
          }
        }
        if (part != null && total != null) {
          percentage = part / total * 100;
          return new AbstractMap.SimpleEntry<>(percentageEvalNode.getAs(), percentage);
        }
      }
      throw new IllegalArgumentException("percentage part/total not found in metric.");
    }
  }
}
