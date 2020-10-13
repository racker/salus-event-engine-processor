This module is part of the Salus system. More information can be found in the 
[bundle repository](https://github.com/racker/salus-telemetry-bundle).

## Event Processing Pipeline

1. Ingest `UniversalMetricFrame` from Kafka
1. Convert to a `EnrichedMetric` subclass e.g. `SalusEnrichedMetric`
1. Send metric to Esper to match against Task Queries
    1. The state will be calculated
    1. Metric states will be stored for each `tenantId:resourceId:monitorId:taskId:zoneId`
1. Once the expected number of consecutive states has been seen for that grouping
    1. All metric states for `tenantId:resourceId:monitorId:taskId` will be sent to the esper event handler
        1. i.e. a list of events containing one event per zone
1. Event handler filters out any old states from the list
1. Event handler verifies enough state events have been seen to evaluate quorum
    1. A single event is enough for local monitor metrics
    1. Remote monitor metrics require the same number of events as zones configured
1. The quorum state will be calculated
1. If the new state differs from the old,
    1. A state change is stored in sql
    1. A state change event is sent to Kafka

## Esper Windows

### EntryWindow

This stores all new metrics that have been matched against a Task Query.  Prior to being added into the window, the metric will be enriched to include the `taskId`, the required number of consecutive states seen before changing to a new state (`expectedStateCounts`), and the newly evaluate `state` (critical, warning, ok).

### StateCountTable

This table keeps track of the current state of an event and the number of times in a row it has been the same state.

If the state changes the counter is reset to 1 for that event key.

This table is updated every time a new event is seen in the `EntryWindow`.

### StateCountSatisfiedWindow

Once the value stored within `StateCountTable` is greater or equal to the corresponding value in an event's `expectedStateCounts` map it will be inserted into this window.

This window is also updated every time a new event is seen in the `EntryWindow`.

There is a listener attached to this window that groups events based on `tenantId:resourceId:monitorId:taskId` and sends them over to the Esper Event Handler for further processing.


## Caching

SQL requests are required as part of the Esper Event Handler to gather a few pieces of information to help determine whether events are valid and whether a state change has occurred or not.

To avoid putting unnecessary stress on MySQL, a caching layer is implemented on top of any repository method call.

The initial SQL request will occur for each event as soon as it has satisfied the state count requirements in at least one monitoring zone.  After that the same methods will be triggered but the repositories' previous result will be used instead.

Without caching, MySQL would be queried for almost every new metric ingested by the event processor.

These caches should not be TTL'd and should be able to contain values for all the metrics/events a single event processing node might have to process.