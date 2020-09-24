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

package com.rackspace.salus.event.processor.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.fireandforget.EPFireAndForgetQueryResult;
import com.espertech.esper.common.client.scopetest.EPAssertionUtil;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.espertech.esper.runtime.client.scopetest.SupportUpdateListener;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.event.processor.services.EsperEngine;
import java.util.Collections;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.annotation.DirtiesContext;

@Slf4j
@DirtiesContext
public class EsperQueryTest {

  @Before
  public void setup() {
    Configuration configuration = new Configuration();
    configuration.getCommon().addEventType(SalusEnrichedMetric.class);
    EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    String basicInsertEntryWindowQuery = getBasicInsertQuery(metric);

    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.CREATE_ENTRY_WINDOW);
    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.CREATE_STATE_COUNT_TABLE)
        .addListener((newData, oldData, stmt, rt) ->
        log.info("Saw new event in count table={}", stmt));
    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.CREATE_STATE_COUNT_SATISFIED_WINDOW);
    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.CREATE_QUORUM_STATE_WINDOW);

    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.UPDATE_STATE_COUNT_LOGIC);
    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.STATE_COUNT_SATISFIED_LOGIC);
//    EsperEngine.compileAndDeployQuery(runtime, configuration, EsperQuery.QUORUM_STATE_LOGIC);
  }

  @After
  public void destroy() throws EPUndeployException {
    EsperEngine.undeployAll(EPRuntimeProvider.getDefaultRuntime());
  }

  /**
   * Used to test the basic query logic and not the extra java methods
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

  @Test
  public void testEntryWindow() {
    Configuration configuration = new Configuration();
    configuration.getCommon().addEventType(SalusEnrichedMetric.class);
    EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    EsperEngine.compileAndDeployQuery(runtime, configuration, getBasicInsertQuery(metric));

    EPStatement stmt = EsperEngine.compileAndDeployQuery(runtime, configuration,
        "@Audit select * from EntryWindow");
    SupportUpdateListener listener = new SupportUpdateListener();
    stmt.addListener(listener);

    runtime.getEventService().sendEventBean(metric, SalusEnrichedMetric.class.getSimpleName());

    EPAssertionUtil.assertProps(listener.assertOneGetNewAndReset(),
        new String[]{"tenantId", "monitorId", "state"},
        new Object[]{metric.getTenantId(), metric.getMonitorId(), metric.getState()});
  }

  @Test
  public void testUpdateStateCountTable() {
    Configuration configuration = new Configuration();
    configuration.getCommon().addEventType(SalusEnrichedMetric.class);
    EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    EsperEngine.compileAndDeployQuery(runtime, configuration, getBasicInsertQuery(metric));

    runtime.getEventService().sendEventBean(metric, SalusEnrichedMetric.class.getSimpleName());
    // tables do not emit events on select so an on demand query is used instead
    EPFireAndForgetQueryResult result = EsperEngine.runOnDemandQuery(runtime, configuration,
        "@Audit select * from StateCountTable");

    assertThat(result.getArray()).hasSize(1);
    EPAssertionUtil.assertProps(result.getArray()[0],
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState(), 1});

    // sending another of the same metric increases the count
    runtime.getEventService().sendEventBean(metric, SalusEnrichedMetric.class.getSimpleName());
    result = EsperEngine.runOnDemandQuery(runtime, configuration,
        "@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(1);
    EPAssertionUtil.assertProps(result.getArray()[0],
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState(), 2});

    // sending a different taskId adds a new state count key
    SalusEnrichedMetric newMetric = createSalusEnrichedMetric();;
    EsperEngine.compileAndDeployQuery(runtime, configuration, getBasicInsertQuery(newMetric));

    runtime.getEventService().sendEventBean(newMetric, SalusEnrichedMetric.class.getSimpleName());
    result = EsperEngine.runOnDemandQuery(runtime, configuration,
        "@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(2);
    EPAssertionUtil.assertPropsPerRowAnyOrder(result.getArray(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[][]{
            {metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
                metric.getZoneId(), metric.getState(), 2},
            {newMetric.getTenantId(), newMetric.getResourceId(), newMetric.getMonitorId(), newMetric.getTaskId(),
            newMetric.getZoneId(), newMetric.getState(), 1}});

    // sending the original metric but with a different state resets it to 1
    runtime.getEventService().sendEventBean(metric.setState("ok"), SalusEnrichedMetric.class.getSimpleName());
    result = EsperEngine.runOnDemandQuery(runtime, configuration,
        "@Audit select * from StateCountTable");
    assertThat(result.getArray()).hasSize(2);
    EPAssertionUtil.assertPropsPerRowAnyOrder(result.getArray(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state", "currentCount"},
        new Object[][]{
            {metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
                metric.getZoneId(), metric.getState(), 1},
            {newMetric.getTenantId(), newMetric.getResourceId(), newMetric.getMonitorId(), newMetric.getTaskId(),
                newMetric.getZoneId(), newMetric.getState(), 1}});

  }

  @Test
  public void testUpdateStateCountSatisfiedWindow() {
    Configuration configuration = new Configuration();
    configuration.getCommon().addEventType(SalusEnrichedMetric.class);
    EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

    SalusEnrichedMetric metric = createSalusEnrichedMetric();
    EsperEngine.compileAndDeployQuery(runtime, configuration, getBasicInsertQuery(metric));

    EPStatement stmt = EsperEngine.compileAndDeployQuery(runtime, configuration,
        "@Audit select * from StateCountSatisfiedWindow");
    SupportUpdateListener listener = new SupportUpdateListener();
    stmt.addListener(listener);

    runtime.getEventService().sendEventBean(metric, SalusEnrichedMetric.class.getSimpleName());
    runtime.getEventService().sendEventBean(metric, SalusEnrichedMetric.class.getSimpleName());

    EPAssertionUtil.assertProps(listener.assertOneGetNewAndReset(),
        new String[]{"tenantId", "resourceId", "monitorId", "taskId", "zoneId", "state"},
        new Object[]{metric.getTenantId(), metric.getResourceId(), metric.getMonitorId(), metric.getTaskId(),
            metric.getZoneId(), metric.getState()});
  }

  private SalusEnrichedMetric createSalusEnrichedMetric() {
    String resourceId = RandomStringUtils.randomAlphabetic(5);
    String monitorId = RandomStringUtils.randomAlphabetic(5);
    String taskId = RandomStringUtils.randomAlphabetic(5);
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
        .setState("critical")
        .setExpectedStateCounts(Map.of(
            "test", 1,
            "critical", 2,
            "ok", 3
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

}
