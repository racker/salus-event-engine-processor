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
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.processor.config.AppProperties;
import com.rackspace.salus.event.processor.config.MultipleConsumerTestConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * These tests verify the coming and going of different consumers.
 *
 * The partition re-assignment algorithm appears to be consistent which allows us to
 * hardcode the assertion values in each test.
 * Without that our assertions would have to be much more generalized.
 *
 * Test will fail if either the number of partitions or the partition strategy is changed.
 */
@Slf4j
@RunWith(SpringRunner.class)
@EmbeddedKafka(partitions = 3, topics = {"telemetry.metrics.json"})
@SpringBootTest(
    properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    classes = {
        KafkaAutoConfiguration.class, MultipleConsumerTestConfig.class,
        AppProperties.class, KafkaTopicProperties.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
public class UniversalMetricListenerTest {

  @Autowired
  AppProperties appProperties;

  long testTimeout;

  // three different consumers are created in MultipleConsumerTestConfig.class

  @Autowired
  @Qualifier("consumer1")
  UniversalMetricListener consumer1;

  @Autowired
  @Qualifier("consumer2")
  UniversalMetricListener consumer2;

  @Autowired
  @Qualifier("consumer3")
  UniversalMetricListener consumer3;

  // each consumer has its own partitionTracker, created in MultipleConsumerTestConfig.class

  @Autowired
  @Qualifier("tracker1")
  PartitionTracker tracker1;

  @Autowired
  @Qualifier("tracker2")
  PartitionTracker tracker2;

  @Autowired
  @Qualifier("tracker3")
  PartitionTracker tracker3;

  @MockBean
  UniversalMetricHandler handler;

  @Before
  public void setup() {
    // increase the delay to help slow laptops running tests locally
    testTimeout = appProperties.getPartitionAssignmentDelay().plus(Duration.ofSeconds(3)).toMillis();
  }

  @Test
  public void testInitialStartup_1consumer() {
    consumer1.start();
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(0, 1, 2));
    verify(handler, timeout(testTimeout)).removeTasksForPartitions(Collections.emptySet());
    verifyNoMoreInteractions(handler);

    assertThat(tracker1.getCurrentAssignedPartitions()).hasSize(3);
    assertThat(tracker1.getCurrentAssignedPartitions()).isEqualTo(tracker1.getTrackedPartitions());
  }

  @Test
  public void testInitialStartup_2consumers() {
    consumer1.start();
    consumer2.start();

    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(0, 2));
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(1));
    verify(handler, timeout(testTimeout).times(2)).removeTasksForPartitions(Collections.emptySet());
    verifyNoMoreInteractions(handler);

    assertThat(tracker1.getCurrentAssignedPartitions()).hasSize(2);
    assertThat(tracker2.getCurrentAssignedPartitions()).hasSize(1);

    assertThat(tracker1.getCurrentAssignedPartitions()).isEqualTo(tracker1.getTrackedPartitions());
    assertThat(tracker2.getCurrentAssignedPartitions()).isEqualTo(tracker2.getTrackedPartitions());
  }

  @Test
  public void testInitialStartup_3consumers() {
    consumer1.start();
    consumer2.start();
    consumer3.start();

    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(0));
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(1));
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(2));
    verify(handler, timeout(testTimeout).times(3)).removeTasksForPartitions(Collections.emptySet());
    verifyNoMoreInteractions(handler);

    assertThat(tracker1.getCurrentAssignedPartitions()).hasSize(1);
    assertThat(tracker2.getCurrentAssignedPartitions()).hasSize(1);
    assertThat(tracker2.getCurrentAssignedPartitions()).hasSize(1);

    assertThat(tracker1.getCurrentAssignedPartitions()).isEqualTo(tracker1.getTrackedPartitions());
    assertThat(tracker2.getCurrentAssignedPartitions()).isEqualTo(tracker2.getTrackedPartitions());
    assertThat(tracker3.getCurrentAssignedPartitions()).isEqualTo(tracker3.getTrackedPartitions());
  }

  @Test
  public void testReassignments_addingConsumers() {
    consumer1.start();
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(0, 1, 2));
    verify(handler, timeout(testTimeout)).removeTasksForPartitions(Collections.emptySet());
    verifyNoMoreInteractions(handler);
    reset(handler);

    consumer2.start();
    // consumer1 adds 0 partitions and removes 1
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Collections.emptySet());
    verify(handler, timeout(testTimeout)).removeTasksForPartitions(Set.of(1));
    // consumer2 adds 1 partition and removes 0
    verify(handler, timeout(testTimeout)).deployTasksForPartitions(Set.of(1));
    verify(handler, timeout(testTimeout)).removeTasksForPartitions(Collections.emptySet());

    verifyNoMoreInteractions(handler);
    reset(handler);

    consumer3.start();
    // sticky partition assignment means a consumer with only 2 more partitions than another
    // will not release any partitions
    verifyNoMoreInteractions(handler);
    reset(handler);

    assertThat(tracker1.getCurrentAssignedPartitions()).hasSize(2);
    assertThat(tracker2.getCurrentAssignedPartitions()).hasSize(1);
    assertThat(tracker3.getCurrentAssignedPartitions()).isEmpty();

    assertThat(tracker1.getCurrentAssignedPartitions()).isEqualTo(tracker1.getTrackedPartitions());
    assertThat(tracker2.getCurrentAssignedPartitions()).isEqualTo(tracker2.getTrackedPartitions());
    assertThat(tracker3.getCurrentAssignedPartitions()).isEqualTo(tracker3.getTrackedPartitions());
  }

  @Test
  public void testReassignments_removingConsumers() {
    consumer1.start();
    consumer2.start();
    consumer3.start();

    verify(handler, timeout(testTimeout).times(3)).deployTasksForPartitions(anySet());
    verify(handler, timeout(testTimeout).times(3)).removeTasksForPartitions(anySet());
    reset(handler);

    consumer1.stop();
    // rebalance is triggered and hits all consumers
    // but that doesn't mean all partitions are moved (due to sticky assignment)
    verify(handler, timeout(testTimeout).times(3)).deployTasksForPartitions(anySet());
    verify(handler, timeout(testTimeout).times(3)).removeTasksForPartitions(anySet());

    assertThat(tracker1.getCurrentAssignedPartitions()).isEmpty();
    assertThat(tracker2.getCurrentAssignedPartitions()).hasSize(2);
    assertThat(tracker3.getCurrentAssignedPartitions()).hasSize(1);

    assertThat(tracker1.getTrackedPartitions()).isEmpty();
    assertThat(tracker2.getCurrentAssignedPartitions()).isEqualTo(tracker2.getTrackedPartitions());
    assertThat(tracker3.getCurrentAssignedPartitions()).isEqualTo(tracker3.getTrackedPartitions());
  }

}