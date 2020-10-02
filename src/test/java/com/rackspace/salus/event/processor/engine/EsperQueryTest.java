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

package com.rackspace.salus.event.processor.engine;

import static com.rackspace.salus.event.processor.utils.TestDataGenerators.createSalusEnrichedMetric;
import static org.assertj.core.api.Assertions.assertThat;

import com.espertech.esper.common.client.fireandforget.EPFireAndForgetQueryResult;
import com.espertech.esper.common.client.scopetest.EPAssertionUtil;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.espertech.esper.runtime.client.scopetest.SupportUpdateListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.event.processor.services.EsperEventsListener;
import com.rackspace.salus.event.processor.services.TaskWarmthTracker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
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
    EsperEngine.class, SimpleMeterRegistry.class, ObjectMapper.class})
public class EsperQueryTest {

  @Autowired
  EsperEngine esperEngine;

  @MockBean
  TaskWarmthTracker warmthTracker;

  @MockBean
  EsperEventsListener eventsListener;

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
  public void testUpdateStateCountSatisfiedWindow() {
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

  @Test
  public void testStateCountSatisfiedListener() {
    SalusEnrichedMetric metric1 = createSalusEnrichedMetric().setZoneId("zone1");
    SalusEnrichedMetric metric2 = SerializationUtils.clone(metric1).setZoneId("zone2");

    esperEngine.compileAndDeployQuery(getBasicInsertQuery(metric1));

    EPStatement stmt = esperEngine.compileAndDeployQuery(EsperQuery.STATE_COUNT_SATISFIED_LISTENER);
    SupportUpdateListener listener = new SupportUpdateListener();
    stmt.addListener(listener);

    // the event will not enter the window until the state count has been satisfied for a single zone
    esperEngine.sendMetric(metric1);
    esperEngine.sendMetric(metric2);
    listener.assertNotInvoked();

    // a second event for a seen zone will trigger the listener
    esperEngine.sendMetric(metric1);

    Map<String, SalusEnrichedMetric[]> queryResponse =
        (HashMap<String, SalusEnrichedMetric[]>) listener.assertOneGetNewAndReset().getUnderlying();
    SalusEnrichedMetric[] metrics = queryResponse.get("eventsForTask");
    assertThat(metrics).hasSize(1);
    assertThat(metrics[0]).isEqualTo(metric1);

    // a second event for the other zone should trigger a unique event for each zone to be seen
    esperEngine.sendMetric(metric2);

    queryResponse =
        (HashMap<String, SalusEnrichedMetric[]>) listener.assertOneGetNewAndReset().getUnderlying();
    metrics = queryResponse.get("eventsForTask");
    assertThat(metrics).hasSize(2);

    assertThat(metrics).containsExactlyInAnyOrder(metric1, metric2);
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
