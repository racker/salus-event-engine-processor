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
import com.rackspace.salus.common.config.MetricNames;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EsperEventsListener implements UpdateListener {

  private final EsperEventsHandler handler;
  private final Counter unexpectedEsperEventType;
  private final Counter unexpectedEsperEventForZone;

  @Autowired
  public EsperEventsListener(EsperEventsHandler handler, MeterRegistry meterRegistry) {
    this.handler = handler;

    unexpectedEsperEventType = meterRegistry.counter(MetricNames.SILENT_ERRORS,
        MetricTags.SERVICE_METRIC_TAG, "EsperEventListener",
        MetricTags.REASON, "unexpected_esper_event_type");

    unexpectedEsperEventForZone = meterRegistry.counter(MetricNames.SILENT_ERRORS,
        MetricTags.SERVICE_METRIC_TAG, "EsperEventListener",
        MetricTags.REASON, "duplicate_zones_in_insert_stream");
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

    List<SalusEnrichedMetric> salusEvents = new ArrayList<>();
    Set<String> seenZones = new HashSet<>();


    for (EventBean event : insertStream) {
      if (event.getUnderlying() instanceof SalusEnrichedMetric) {
        SalusEnrichedMetric salusEvent = (SalusEnrichedMetric) event.getUnderlying();

        // only use one event per zone
        // the esper query should not allow for multiple
        if (seenZones.add(salusEvent.getZoneId())) {
          salusEvents.add(salusEvent);
        } else {
          log.error("Multiple events of zone={} were seen in insertStream for event={}",
              salusEvent.getZoneId(), salusEvent);
          unexpectedEsperEventForZone.increment();
        }
      } else {
        log.error("Received unexpected event type={} from esper",
            event.getUnderlying().getClass().getSimpleName());
        unexpectedEsperEventType.increment();
      }
    }

    handler.processEsperEvents(salusEvents);

  }
}
