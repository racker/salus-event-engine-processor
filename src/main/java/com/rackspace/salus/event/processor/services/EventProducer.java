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

import static com.rackspace.salus.telemetry.messaging.KafkaMessageKeyBuilder.buildMessageKey;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.telemetry.messaging.StateChangeEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * Consolidates Kafka event producing operations
 */
@Service
public class EventProducer {

  private final KafkaTemplate<String,Object> kafkaTemplate;
  private final KafkaTopicProperties kafkaTopics;

  @Autowired
  public EventProducer(KafkaTemplate<String,Object> kafkaTemplate, KafkaTopicProperties kafkaTopics) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaTopics = kafkaTopics;
  }

  ListenableFuture<SendResult<String, Object>> sendStateChange(StateChangeEvent event) {
    return kafkaTemplate.send(
        kafkaTopics.getStateChangeEvents(),
        buildMessageKey(event),
        event
    );
  }
}
