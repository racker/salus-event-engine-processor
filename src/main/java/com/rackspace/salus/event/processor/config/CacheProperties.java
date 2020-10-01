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

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("salus.event-processor.cache")
public class CacheProperties {
   /*
    * This value needs to be large enough to hold keys for each of these scenarios:
    *
    * 1. All monitors an event-processor may process metrics for
    * 2. All monitors per resource per tenant an event-processor may process metrics for
    * 3. All tasks per monitor per resource per tenant an event-processor may handle state changes for
    *
    * Therefore the 3rd scenario is the one that really dictates this value.
    */
  long maxSize = 2_000;

  // TODO : is it better to set three different cache configurations instead of using the max heap
  // value for all of them?

}