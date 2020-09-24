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

import com.rackspace.salus.event.processor.model.SalusEnrichedMetric;

public class EsperQuery {

  private static final String SALUS_EVENT_NAME = SalusEnrichedMetric.class.getSimpleName();

  // TODO: Do we need to use @Priority in queries to define order?
  // The runtime, by default, does not guarantee to execute competing statements in any particular order unless using @Priority

  public static final String CREATE_ENTRY_WINDOW = ""
      + "create window EntryWindow.std:unique("
      + "   tenantId, resourceId, monitorId, taskId, zoneId) as "
      + "select * from " + SALUS_EVENT_NAME; // defines the schema for stored events

  public static final String CREATE_STATE_COUNT_TABLE = ""
      + "create table StateCountTable ("
      + "   tenantId string primary key,"
      + "   resourceId string primary key,"
      + "   monitorId string primary key,"
      + "   taskId string primary key,"
      + "   zoneId string primary key,"
      + "   state string,"
      + "   currentCount int"
      + ")";

  public static final String CREATE_STATE_COUNT_SATISFIED_WINDOW = ""
      + "create window StateCountSatisfiedWindow.std:unique("
      + "   tenantId, resourceId, monitorId, taskId, zoneId) as "
      + "select * from " + SALUS_EVENT_NAME; // defines the schema for stored events

  public static final String CREATE_QUORUM_STATE_WINDOW = ""
      + "create window QuorumStateWindow.std:unique(tenantId, resourceId, monitorId, taskId) as "
      + "select * from " + SALUS_EVENT_NAME; // defines the schema for stored events

  public static final String UPDATE_STATE_COUNT_LOGIC = ""
      + "on EntryWindow as ew "
      + "merge into StateCountTable as sct "
      + "where "
      + "   ew.tenantId = sct.tenantId and "
      + "   ew.resourceId = sct.resourceId and "
      + "   ew.monitorId = sct.monitorId and "
      + "   ew.taskId = sct.taskId and "
      + "   ew.zoneId = sct.zoneId "
      + "when matched and ew.state = sct.state "
      + "   then update set sct.currentCount = sct.currentCount + 1 "
      // TODO: should we prevent currentCount from getting too high?  What happens when it maxes out?
      + "when matched and ew.state != sct.state "
      + "   then update set sct.currentCount = 1, sct.state = ew.state "
      + "when not matched "
      + "   then insert (tenantId, resourceId, monitorId, taskId, zoneId, state, currentCount) "
      + "        select tenantId, resourceId, monitorId, taskId, zoneId, state, 1";

  public static final String STATE_COUNT_SATISFIED_LOGIC = ""
      + "insert into StateCountSatisfiedWindow "
      + "select ew.* from EntryWindow as ew "
      + "   where "
      + "       ew.expectedStateCounts(ew.state) = 1 or" // TODO: this step might not actually help with any optimization
      + "       (select sct.currentCount from StateCountTable as sct "
      + "          where "
      + "              ew.tenantId = sct.tenantId and "
      + "              ew.resourceId = sct.resourceId and "
      + "              ew.monitorId = sct.monitorId and "
      + "              ew.taskId = sct.taskId and "
      + "              ew.zoneId = sct.zoneId"
      + "       ) >= ew.expectedStateCounts(ew.state)"; // TODO: can we guarantee order of updates better?

  public static final String QUORUM_STATE_LOGIC = ""
      + "insert into QuorumStateWindow "
      + "select StateEvaluator.quorumState(window(*)) "
      + "   from StateCountSatisfiedWindow as scsw "
      + "   group by scsw.tenantId, scsw.resourceId, scsw.monitorId, scsw.taskId "
      + "   having count(*) > 1 "; // TODO: is this line needed?
}
