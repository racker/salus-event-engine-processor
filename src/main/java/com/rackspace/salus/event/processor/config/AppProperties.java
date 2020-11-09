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

import java.time.Duration;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("salus.event-processor")
@Component
@Data
@Validated
public class AppProperties {

  /**
   * Whether the metrics consumer should begin automatically on service startup
   */
  Boolean kafkaListenerAutoStart = true;

  /**
   * The time to wait after a partition reassignment before taking action.
   *
   * The value should be set large enough to allow for normal amounts of partition shuffling
   * when a new consumer is added into the group and/or an existing one goes offline.
   *
   * In production it could be set to 1min or greater.
   */
  Duration partitionAssignmentDelay = Duration.ofSeconds(60);

  String metricsConsumerListenerId = RandomStringUtils.randomAlphabetic(10);
}
