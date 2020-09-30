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

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import com.rackspace.salus.event.processor.services.EsperEventsHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EsperEventsListener implements UpdateListener {

  private EsperEventsHandler handler;
  private MeterRegistry meterRegistry;
  private Counter unexpectedEsperEvents;

  @Autowired
  public EsperEventsListener(EsperEventsHandler handler, MeterRegistry meterRegistry) {
    this.handler = handler;
    this.meterRegistry = meterRegistry;

    unexpectedEsperEvents = meterRegistry.counter("unexpected_esper_metrics",
        MetricTags.SERVICE_METRIC_TAG, "EsperEventListener");
  }

  /**
   *
   * @param insertStream The list of events returned by EVENT_STATE_LISTENER
   * @param removeStream
   * @param epStatement
   * @param epRuntime
   */
  @Override
  public void update(EventBean[] insertStream, EventBean[] removeStream, EPStatement epStatement,
      EPRuntime epRuntime) {
    log.debug("Received {} state change events from esper", insertStream.length);
    List<SalusEnrichedMetric> events = Arrays.asList(insertStream).stream()
        .map(event -> {
          if (event.getUnderlying() instanceof SalusEnrichedMetric) {
            return (SalusEnrichedMetric) event.getUnderlying();
          }
          log.error("Received unexpected event type={} from esper",
              event.getUnderlying().getClass().getSimpleName());
          unexpectedEsperEvents.increment();
          return null;
        })
        .collect(Collectors.toList());

    handler.processEsperEvents(events);

  }
}
