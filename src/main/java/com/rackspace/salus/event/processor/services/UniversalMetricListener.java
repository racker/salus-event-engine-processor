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

import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.monplat.protocol.UniversalMetricFrame.MonitoringSystem;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UniversalMetricListener {

  private final UniversalMetricHandler handler;
  private final KafkaTopicProperties properties;
  private final String topic;

  @Autowired
  public UniversalMetricListener(
      UniversalMetricHandler handler,
      KafkaTopicProperties properties) {
    this.handler = handler;
    this.properties = properties;
    this.topic = properties.getMetrics();
  }

  /**
   * This method is used by the __listener.topic magic in the KafkaListener
   *
   * @return The topic to consume
   */
  public String getTopic() {
    return this.topic;
  }

  /**
   * This receives a UniversalMetricFrame event from Kafka and passes it on to
   * another service to process it.
   *
   * @param metric The UniversalMetricFrame read from Kafka.
   */
  @KafkaListener(topics = "#{__listener.topic}")
  public void consumeUniversalMetrics(UniversalMetricFrame metric) {
    log.debug("Processing kapacitor event: {}", metric);
    if (metric.getMonitoringSystem().equals(MonitoringSystem.SALUS)) {
      handler.processSalusMetricFrame(metric);
    }
  }
}
