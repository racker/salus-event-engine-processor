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
import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.salus.event.processor.model.EnrichedMetric;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.event.processor.services.EsperEventsListener;
import com.rackspace.salus.event.processor.services.StateEvaluator;
import com.rackspace.salus.event.processor.services.TaskWarmthTracker;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EsperEngine {

  private final EPRuntime runtime;
  private final Configuration config;
  private TaskWarmthTracker taskWarmthTracker;
  private EsperEventsListener esperEventsListener;
  private final EventEngineTaskRepository eventEngineTaskRepository;

  @Autowired
  public EsperEngine(TaskWarmthTracker taskWarmthTracker,
      EsperEventsListener esperEventsListener, Environment env, EventEngineTaskRepository eventEngineTaskRepository) {
    this.taskWarmthTracker = taskWarmthTracker;
    this.esperEventsListener = esperEventsListener;
    this.eventEngineTaskRepository = eventEngineTaskRepository;

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
    loadTasks();
    
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
        "    monitoringSystem='salus' and\n" +
        "    tenantId='%s'%s) metric";

    String eplPrevTemplate = "@name('%s:%s')\n" +
        "insert into EntryWindow\n" +
        "select StateEvaluator.evalMetricState(metric, prev(1, metric), '%s') " +
        "from SalusEnrichedMetric(" +
        // TODO: fix monitoringSystem etc when other fields are added
        "    monitoringSystem='salus' and\n" +
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
    String taskId = t.getId().toString();
    String deploymentId = StateEvaluator.removeTaskData(taskId);
    if (deploymentId != null) {
      try {
        runtime.getDeploymentService().undeploy(deploymentId);
        log.trace("Removing task for tenant={} task={}", t.getTenantId(), taskId);
      } catch (EPUndeployException e) {
        log.trace("Exception removing task for tenant={} task={} exception message={}",
          t.getTenantId(), taskId, e.getMessage());
      }
    }
  }


  public void loadTasks() {
    String tenantId = "aaaaaa";
    Pageable p = PageRequest.of(0, 10);
    Page<EventEngineTask> page = eventEngineTaskRepository.findByTenantId(tenantId, p);
    log.info("gbj number of entries: " + page.getNumberOfElements());
    page.get().forEach(this::addTask);
    log.info("gbj finished load.");
    // gbj remove:
    try {
      Thread.sleep(2000);
    } catch (java.lang.InterruptedException e) {};
    sendEvents();
  }

  static long timeCount = Instant.now().getEpochSecond() - 600;
  static int totalCount;
  private static UUID myuuid = UUID.randomUUID();
  private static SalusEnrichedMetric buildMetric(String tenantId, String resourceId, Map<String, String> tags) {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(timeCount).build();
    timeCount += 60;
    Metric part = Metric.newBuilder().setName("part").setInt(2).setTimestamp(timestamp).build();
    Metric total = Metric.newBuilder().setName("total").setInt(totalCount).setTimestamp(timestamp).build();
    totalCount += 120;
    Metric totalCpu = Metric.newBuilder().setName("total_cpu").setInt(5).setTimestamp(timestamp).build();
 //   List<Metric> list = List.of(part, total, totalCpu);
    List<Metric> list = new ArrayList<Metric>(Arrays.asList(part, total, totalCpu));
 
    SalusEnrichedMetric s =  new SalusEnrichedMetric();


      s.setResourceId("my-resource")
      .setMonitorId(myuuid)
      .setZoneId("dfw")
      .setMonitorType("http")
      .setMonitorSelectorScope("remote")
      .setMonitoringSystem("salus")
      .setTags(tags != null ? tags : Map.of(
            "os", "linux",
            "metric", "something"))
      .setMetrics(list)
      .setTenantId("aaaaaa");

    return s;
  }

  public void sendEvents() {
    int metricRange = 10;
    SalusEnrichedMetric m1 = buildMetric(null, null, null);
    SalusEnrichedMetric m2 = buildMetric(null, null, null);

    // send {metricRange} dupes of m1
    // send {metricRange} dupes of m2
    IntStream.range(0, metricRange).forEach(i -> {
      log.info("sending valid metric 1-{}", i);
      runtime.getEventService().sendEventBean(buildMetric(null, null, null), "SalusEnrichedMetric");
//      log.info("sending valid metric 2-{}", i);
//      runtime.getEventService().sendEventBean(m2, "SalusEnrichedMetric");
    });
  }

}