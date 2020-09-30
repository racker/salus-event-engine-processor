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

import com.espertech.esper.common.client.fireandforget.EPFireAndForgetQueryResult;
import com.espertech.esper.common.client.scopetest.EPAssertionUtil;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.espertech.esper.runtime.client.scopetest.SupportUpdateListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.event.processor.engine.EsperEventsListener;
import com.rackspace.salus.event.processor.engine.StateEvaluator;
import com.rackspace.salus.event.processor.engine.TaskWarmthTracker;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.repositories.BoundMonitorRepository;
import com.rackspace.salus.telemetry.repositories.MonitorRepository;
import com.rackspace.salus.telemetry.repositories.StateChangeRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
    EsperEngine.class, StateEvaluator.class, TaskWarmthTracker.class,
    EsperEventsHandler.class, EsperEventsListener.class,
    SimpleMeterRegistry.class, ObjectMapper.class})
public class EsperEngineTest {

  @Autowired
  EsperEngine esperEngine;

  @Autowired
  EsperEventsListener eventsListener;

  @Autowired
  EsperEventsHandler eventsHandler;

  @MockBean
  MonitorRepository monitorRepository;

  @MockBean
  StateChangeRepository stateChangeRepository;

  @MockBean
  BoundMonitorRepository boundMonitorRepository;


  @Before
  public void setup() {
    esperEngine.initialize();
  }
  @After
  public void destroy() throws EPUndeployException {
    esperEngine.undeployAll();
  }

  @Test
  public void testEntryWindow() throws EPUndeployException {
    // we do not want to test anything beyond entry window so we can remove the connecting queries
    esperEngine.undeploy("stateCountTableLogic");
    esperEngine.undeploy("stateCountSatisfiedLogic");
    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    esperEngine.compileAndDeployQuery(getBasicInsertQuery(metric));

    EPStatement stmt = esperEngine.compileAndDeployQuery(
        "@Audit select * from EntryWindow");
    SupportUpdateListener listener = new SupportUpdateListener();
    stmt.addListener(listener);

    esperEngine.sendMetric(metric);

    EPAssertionUtil.assertProps(listener.assertOneGetNewAndReset(),
        new String[]{"tenantId", "monitorId", "state"},
        new Object[]{metric.getTenantId(), metric.getMonitorId(), metric.getState()});
  }

  /**
   * Verify the tracked state counts are updated correctly upon seeing various events.
   */
  @Test
  public void testUpdateStateCountTable() throws EPUndeployException {
    // we do not want to test anything beyond state count logic so we can remove the connecting queries
    esperEngine.undeploy("stateCountSatisfiedLogic");

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    esperEngine.compileAndDeployQuery(getBasicInsertQuery(metric));

    esperEngine.sendMetric(metric);
    // tables do not emit events on select so an on demand query is used instead
    EPFireAndForgetQueryResult result = esperEngine.runOnDemandQuery("@Audit select * from StateCountTable");

    assertThat(result.getArray()).hasSize(1);
    EPAssertionUtil.assertProps(result.getArray()[0],
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState(), 1});

    // sending another of the same metric increases the count
    esperEngine.sendMetric(metric);
    result = esperEngine.runOnDemandQuery("@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(1);
    EPAssertionUtil.assertProps(result.getArray()[0],
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState(), 2});

    // sending a different taskId adds a new state count key
    SalusEnrichedMetric newMetric = createSalusEnrichedMetric();;
    esperEngine.compileAndDeployQuery(getBasicInsertQuery(newMetric));

    esperEngine.sendMetric(newMetric);
    result = esperEngine.runOnDemandQuery("@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(2);
    EPAssertionUtil.assertPropsPerRowAnyOrder(result.getArray(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[][]{
            {metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
                metric.getZoneId(), metric.getState(), 2},
            {newMetric.getTenantId(), newMetric.getResourceId(), newMetric.getMonitorId(), newMetric.getTaskId(),
                newMetric.getZoneId(), newMetric.getState(), 1}});

    // sending the original metric but with a different state resets it to 1
    esperEngine.sendMetric(metric.setState("one"));
    result = esperEngine.runOnDemandQuery("@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(2);
    EPAssertionUtil.assertPropsPerRowAnyOrder(result.getArray(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[][]{
            {metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
                metric.getZoneId(), metric.getState(), 1},
            {newMetric.getTenantId(), newMetric.getResourceId(), newMetric.getMonitorId(), newMetric.getTaskId(),
                newMetric.getZoneId(), newMetric.getState(), 1}});

  }

  /**
   * Verify events only enter the state count satisfied window if the expected state
   * count has been observed.
   */
  @Test
  public void testUpdateStateCountSatisfiedWindow() throws EPUndeployException {
    // we do not want to test anything beyond state count logic so we can remove the connecting queries
    esperEngine.undeploy("quorumStateLogic");

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    esperEngine.compileAndDeployQuery(getBasicInsertQuery(metric));

    EPStatement stmt = esperEngine.compileAndDeployQuery(
        "@Audit select * from StateCountSatisfiedWindow");
    SupportUpdateListener listener = new SupportUpdateListener();
    stmt.addListener(listener);

    // the event will not enter the window until the state count has been satisfied
    esperEngine.sendMetric(metric);
    listener.assertNotInvoked();
    esperEngine.sendMetric(metric);

    EPAssertionUtil.assertProps(listener.assertOneGetNewAndReset(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState()});
  }

  private SalusEnrichedMetric createSalusEnrichedMetric() {
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

  /**
   * Used to test the basic query logic and not the extra java methods.
   *
   * @param metric A salus metric to populate the missing query fields
   * @return An epl to be deployed to esper
   */
  private String getBasicInsertQuery(SalusEnrichedMetric metric) {
    return String.format(""
            + "@name('%s') "
            + "insert into EntryWindow "
            + "select * from SalusEnrichedMetric("
            + "   tenantId='%s' and "
            + "   monitorSelectorScope='%s' and "
            + "   monitorType='%s' and "
            + "   resourceId not in (excludedResourceIds) and "
            + "   tags('os')='linux' and tags('dc')='private')",
        metric.getTaskId(),
        metric.getTenantId(),
        metric.getMonitorSelectorScope(),
        metric.getMonitorType());
  }
}