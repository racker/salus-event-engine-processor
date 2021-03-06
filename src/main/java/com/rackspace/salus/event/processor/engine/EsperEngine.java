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

import static org.springframework.util.CollectionUtils.isEmpty;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.fireandforget.EPFireAndForgetPreparedQuery;
import com.espertech.esper.common.client.fireandforget.EPFireAndForgetQueryResult;
import com.espertech.esper.common.client.util.NameAccessModifier;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import com.rackspace.salus.event.processor.model.EnrichedMetric;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.event.processor.services.EsperEventsListener;
import com.rackspace.salus.event.processor.services.StateEvaluator;
import com.rackspace.salus.event.processor.services.TaskWarmthTracker;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EsperEngine {

  private final EPRuntime runtime;
  private final Configuration config;
  private final TaskWarmthTracker taskWarmthTracker;
  private final EsperEventsListener esperEventsListener;

  @Autowired
  public EsperEngine(TaskWarmthTracker taskWarmthTracker,
      EsperEventsListener esperEventsListener, Environment env) {
    this.taskWarmthTracker = taskWarmthTracker;
    this.esperEventsListener = esperEventsListener;

    this.config = new Configuration();
    this.config.getCommon().addEventType(SalusEnrichedMetric.class);
    this.config.getCommon().addImport(StateEvaluator.class);
    // remove thread that handles time advancing
    this.config.getRuntime().getThreading().setInternalTimerEnabled(false);

    // If your application is not a multithreaded application, or your application is not sensitive
    // to the order of delivery of result events to your application listeners, then consider
    // disabling the delivery order guarantees the runtime makes towards ordered delivery of results
    // to listeners:
    // this.config.getRuntime().getThreading().setListenerDispatchPreserveOrder(false);

    // If your application is not a multithreaded application, or your application uses the insert
    // into clause to make results of one statement available for further consuming statements but
    // does not require ordered delivery of results from producing statements to consuming
    // statements, you may disable delivery order guarantees between statements:
    // this.config.getRuntime().getThreading().setInsertIntoDispatchPreserveOrder(false);

    this.runtime = EPRuntimeProvider.getDefaultRuntime(this.config);

    // don't deploy esper queries for tests; it is handled within each test
    if (!env.acceptsProfiles(Profiles.of("test"))) {
      initialize();
    }
  }

  void initialize() {
    createWindows();
    createWindowLogic();
    createListeners();
  }

  private void createWindows() {
    compileAndDeployQuery(EsperQuery.CREATE_ENTRY_WINDOW);
    compileAndDeployQuery(EsperQuery.CREATE_STATE_COUNT_TABLE);
    compileAndDeployQuery(EsperQuery.CREATE_STATE_COUNT_SATISFIED_WINDOW);
  }

  private void createWindowLogic() {
    compileAndDeployQuery(EsperQuery.UPDATE_STATE_COUNT_LOGIC);
    compileAndDeployQuery(EsperQuery.STATE_COUNT_SATISFIED_LOGIC);
  }

  private void createListeners() {
    compileAndDeployQuery(EsperQuery.STATE_COUNT_SATISFIED_LISTENER)
        .addListener(esperEventsListener);
  }

  // TODO: can CompilerArguments be created at the class level?
  EPStatement compileAndDeployQuery(String epl) {
    return compileAndDeployQuery(runtime, config, epl);
  }

  public static EPStatement compileAndDeployQuery(EPRuntime runtime, Configuration config, String epl) {
    try {
      CompilerArguments args = new CompilerArguments(config);
      args.getPath().add(runtime.getRuntimePath());
      args.getOptions().setAccessModifierNamedWindow(env -> NameAccessModifier.PUBLIC); // All named windows are visibile
      args.getOptions().setAccessModifierTable(env -> NameAccessModifier.PUBLIC); // All tables are visibile
      EPCompiled compiled = EPCompilerProvider.getCompiler().compile(epl, args);
      EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);
      return deployment.getStatements()[0];
    } catch (EPCompileException e) {
      log.error("Failed to compile query={}", epl);
      throw new RuntimeException(e);
    } catch (EPDeployException ex) {
      log.error("Failed to deploy query={}", epl);
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("SameParameterValue")
  EPFireAndForgetQueryResult runOnDemandQuery(String epl) {
    return runOnDemandQuery(runtime, config, epl);
  }

  public static EPFireAndForgetQueryResult runOnDemandQuery(EPRuntime runtime, Configuration config, String epl) {
    try {
      CompilerArguments args = new CompilerArguments(config);
      args.getPath().add(runtime.getRuntimePath());
      args.getOptions().setAccessModifierNamedWindow(env -> NameAccessModifier.PUBLIC);
      args.getOptions().setAccessModifierTable(env -> NameAccessModifier.PUBLIC);
      EPCompiled compiled = EPCompilerProvider.getCompiler().compileQuery(epl, args);
      EPFireAndForgetPreparedQuery onDemandQuery = runtime.getFireAndForgetService().prepareQuery(compiled);
      return onDemandQuery.execute();
    } catch (EPCompileException e) {
      log.error("Failed to compile query={}", epl);
      throw new RuntimeException(e);
    }
  }

  // TODO there must be a better way than this
  public void undeploy(String queryName) throws EPUndeployException {
    String[] query = runtime.getDeploymentService().getDeployments();
    for (String q : query) {
      for (EPStatement s : runtime.getDeploymentService().getDeployment(q).getStatements()) {
        if (s.getName().equals(queryName)) {
          runtime.getDeploymentService().undeploy(s.getDeploymentId());
          return;
        }
      }
    }
  }

  public void undeployAll() throws EPUndeployException {
    undeployAll(runtime);
  }

  public static void undeployAll(EPRuntime runtime) throws EPUndeployException {
    runtime.getDeploymentService().undeployAll();
  }

  public void sendMetric(EnrichedMetric metric) {
    log.trace("Sending metric to esper engine, {}", metric);
    runtime.getEventService().sendEventBean(metric, metric.getClass().getSimpleName());
  }

  public String createTaskEpl(EventEngineTask t) {
    String taskId = t.getId().toString();
    String tenantId = t.getTenantId();
    // create "tags('os')='linux' and tags('metric')='something'" string
    Map<String, String> labelSelectors = t.getTaskParameters().getLabelSelector();
    String tagsString = "";
    if (!isEmpty(labelSelectors)) {
      tagsString = t.getTaskParameters().getLabelSelector().entrySet().stream().
        map(e -> "tags('" + e.getKey() + "')='" + e.getValue() + "'").
        collect(Collectors.joining(" and "));
      if (tagsString.length() > 0) {
        tagsString = " and " + tagsString;
      }
    }
    String eplRegularTemplate = "@name('%s:%s')\n" +
        "insert into EntryWindow\n" +
        "select StateEvaluator.evalMetricState(metric, null, '%s') " +
        "from SalusEnrichedMetric(" +
        // TODO: fix monitoringSystem etc when other fields are added
        "    monitoringSystem='SALUS' and\n" +
        "    tenantId='%s'%s) metric;";

    String eplPrevTemplate = "@name('%s:%s')\n" +
        "insert into EntryWindow\n" +
        "select StateEvaluator.evalMetricState(metric, prev(1, metric), '%s') " +
        "from SalusEnrichedMetric(" +
        // TODO: fix monitoringSystem etc when other fields are added
        "    monitoringSystem='SALUS' and\n" +
        "    tenantId='%s'%s).std:groupwin(tenantId, resourceId, monitorId, taskId, zoneId).win:length(2) metric where prev(1, metric) is not null;";
     String eplTemplate;

     if (StateEvaluator.includePrev(t)) {
       eplTemplate = eplPrevTemplate;
     } else {
       eplTemplate = eplRegularTemplate;
     }

     return String.format(eplTemplate, tenantId, taskId,
       taskId, tenantId, tagsString);

  }

  public void addTask(EventEngineTask t) {
    String taskId = t.getId().toString();
    String eplString = createTaskEpl(t);
    EPStatement epStatement = compileAndDeployQuery(eplString);
    StateEvaluator.saveTaskData(taskId, epStatement.getDeploymentId(), t);
    log.trace("Adding task for tenant={} task={}", t.getTenantId(), taskId);
  }

  public void removeTask(EventEngineTask t) {
    removeTask(t.getTenantId(), t.getId());
  }

  public void removeTask(String tenantId, UUID taskId) {
    String deploymentId = StateEvaluator.removeTaskData(taskId.toString());
    if (deploymentId != null) {
      try {
        runtime.getDeploymentService().undeploy(deploymentId);
        log.trace("Removing task for tenant={} task={}", tenantId, taskId);
      } catch (EPUndeployException e) {
        log.trace("Exception removing task for tenant={} task={} exception message={}",
            tenantId, taskId, e.getMessage());
      }
    }
  }
}
