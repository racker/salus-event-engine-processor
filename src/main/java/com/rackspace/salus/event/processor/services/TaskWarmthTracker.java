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

import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskWarmthTracker implements Serializable {
  private static final int RESET_AT = Integer.MAX_VALUE / 2;
  private static final int RESET_TO = 50;

  private final ConcurrentMap<String, AtomicInteger> warmingTasks;

  @Autowired
  public TaskWarmthTracker() {
    this.warmingTasks = new ConcurrentHashMap<>();
  }

  //TODO: Update this to accept a monitor change event instead of string
  public void resetWarmthForTask(String monitorEvent) {
    AtomicInteger warmth = warmingTasks.get(monitorEvent);

    if (warmth != null) {
      warmingTasks.get(monitorEvent).set(0);
    }
  }

  public int getTaskWarmth(SalusEnrichedMetric metric) {
    AtomicInteger warmth = warmingTasks.get(metric.getCompositeKey());

    if (warmth == null) {
      warmth = warmingTasks.putIfAbsent(metric.getCompositeKey(), new AtomicInteger(1));
    }

    if (warmth != null) {
      int value = warmth.incrementAndGet();

      // Once the value reaches RESET_AT, we reset it to RESET_TO
      if (value >= RESET_AT) {
        //Noting that retrieval operations do not block and can overlap with update operations
        warmingTasks.get(metric.getCompositeKey()).set(RESET_TO);
      }

      // We never return more than RESET_TO
      return Math.min(value, RESET_TO);
    } else {
      return 1;
    }
  }
}