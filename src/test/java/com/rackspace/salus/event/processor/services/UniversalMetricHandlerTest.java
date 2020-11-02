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

import static com.rackspace.salus.event.processor.utils.TestDataGenerators.generateUniversalMetricFrame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.espertech.esper.runtime.client.EPUndeployException;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.event.processor.engine.EsperEngine;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.telemetry.entities.subtype.SalusEventEngineTask;
import com.rackspace.salus.telemetry.model.ConfigSelectorScope;
import com.rackspace.salus.telemetry.repositories.SalusEventEngineTaskRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {UniversalMetricHandler.class})
public class UniversalMetricHandlerTest {

  @Autowired
  UniversalMetricHandler handler;

  @MockBean
  EsperEngine esperEngine;

  @MockBean
  SalusEventEngineTaskRepository salusTaskRepository;

  @Captor
  ArgumentCaptor<SalusEnrichedMetric> salusMetricArg;

  private PodamFactory podamFactory = new PodamFactoryImpl();

  @Test
  public void convertUniversalMetricToSalusMetricTest() {
    UniversalMetricFrame universalMetric = generateUniversalMetricFrame();
    SalusEnrichedMetric salusMetric = handler.convertUniversalMetricToSalusMetric(universalMetric);

    SalusEnrichedMetric expectedMetric = (SalusEnrichedMetric) new SalusEnrichedMetric()
        .setResourceId("resource-1")
        .setMonitorId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
        .setTaskId(null)
        .setZoneId("zone-1")
        .setMonitorType("ping")
        .setMonitorSelectorScope(ConfigSelectorScope.REMOTE.toString())
        .setExpectedStateCounts(null)
        .setExcludedResourceIds(null)
        .setStateEvaluationTimestamp(null)
        .setMonitoringSystem("SALUS")
        .setTenantId("123456")
        .setAccountType("MANAGED_HOSTING")
        .setMetrics(universalMetric.getMetricsList())
        .setTags(null);

    assertThat(salusMetric).isEqualTo(expectedMetric);
    assertThat(salusMetric.getMetrics()).hasSize(3);
  }

  @Test
  public void processSalusMetricFrameTest() {
    UniversalMetricFrame universalMetric = generateUniversalMetricFrame();
    handler.processSalusMetricFrame(universalMetric);

    verify(esperEngine).sendMetric(salusMetricArg.capture());

    SalusEnrichedMetric expectedMetric = handler.convertUniversalMetricToSalusMetric(universalMetric);
    assertThat(salusMetricArg.getValue()).isEqualTo(expectedMetric);
  }

  @Test
  public void removeTasksForPartitionsTest() throws EPUndeployException {
    List<SalusEventEngineTask> tasksToRemove = podamFactory.manufacturePojo(ArrayList.class, SalusEventEngineTask.class);
    when(salusTaskRepository.findByPartition(any()))
        .thenReturn(tasksToRemove);

    handler.removeTasksForPartitions(Set.of(2, 4));

    verify(salusTaskRepository).findByPartition(2);
    verify(salusTaskRepository).findByPartition(4);

    // each removed partition should return 5 each
    verify(esperEngine, times(10)).removeTask(argThat(eventEngineTask -> {
      assertThat(eventEngineTask).isIn(tasksToRemove);
      return true;
    }));

    verifyNoMoreInteractions(esperEngine, salusTaskRepository);
  }

  @Test
  public void deployTasksForPartitionsTest() {
    List<SalusEventEngineTask> tasksToDeploy = podamFactory.manufacturePojo(ArrayList.class, SalusEventEngineTask.class);
    when(salusTaskRepository.findByPartition(any()))
        .thenReturn(tasksToDeploy);

    handler.deployTasksForPartitions(Set.of(2, 4));

    verify(salusTaskRepository).findByPartition(2);
    verify(salusTaskRepository).findByPartition(4);

    // each added partition should return 5 each
    verify(esperEngine, times(10)).addTask(argThat(eventEngineTask -> {
      assertThat(eventEngineTask).isIn(tasksToDeploy);
      return true;
    }));

    verifyNoMoreInteractions(esperEngine, salusTaskRepository);
  }
}
