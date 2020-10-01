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
import com.rackspace.salus.event.processor.config.EsperQuery;
import com.rackspace.salus.event.processor.model.EnrichedMetric;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EsperEngine {

  private EPRuntime runtime;
  private Configuration config;
  private TaskWarmthTracker taskWarmthTracker;
  private EsperEventsListener esperEventsListener;

  @Autowired
  public EsperEngine(TaskWarmthTracker taskWarmthTracker,
      EsperEventsListener esperEventsListener) {
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

//    initialize();
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

  void sendMetric(EnrichedMetric metric) {
    log.trace("Sending metric to esper engine, {}", metric);
    runtime.getEventService().sendEventBean(metric, metric.getClass().getSimpleName());
  }
}
