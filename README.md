# Spanners in the works

This is a collection service for span data.

## Required Tech and Installation

Install the following:
- [Docker](https://docs.docker.com/install/)
- [Docker compose](https://docs.docker.com/compose/install/)

Run `docker-compose up`

## Usage

Once the services stop complaining about not being able to talk to each other,
you should be safe to `POST` json to `localhost:12345`

## Request Format

### Query parameters

Note: one of either `license_key` or `insights_key` (or both!) are required.

|     Param      |  Type  | Description |
|----------------|--------|-------------|
| `entity_name`  | string | Name of the entity the trace was produced from.  |
| `license_key`  | string | [*Optional*] If specified, the collector will send span data to New Relic. |
| `insights_key` | string | [*Optional*] If specified, the collector will send metric data to New Relic. |
|  `entity_id`   | string | [*Optional*] ID of the entity the trace was produced from.  |

### Payload
The payload should be JSON with the following fields defined:

|Field|Field Type|Description|
|-----|----------|-----------|
|`span_id`|string|Unique identifier for this span|
|`trace_id`|string|Unique identifier shared by all spans within a single trace|
|`name`|string|The name of this span|
|`parent_id`|string|The span id of the previous caller of this span. Should be omitted for the root span.|
|`start_time`|float|Timestamp for the start of this span in milliseconds|
|`finish_time`|float|Timestamp for the end of this span in milliseconds|
|`tags`|map<string,*>|[*Optional*] Map of user specified "tags" on this span. Keys are strings, values can take any form|

### Example Request

```
curl -X POST -d '[{"trace_id":"fae87301e545a8","span_id":"13d25d1c3216130","name":"query","start_time":1549128157238,"finish_time":1549128157239,"category":"generic","tags":{},"parent_id":"fae87301e545a8"},{"trace_id":"fae87301e545a8","span_id":"b022feeb4e0de","name":"expressInit","start_time":1549128157239,"finish_time":1549128157239,"category":"generic","tags":{},"parent_id":"fae87301e545a8"},{"trace_id":"fae87301e545a8","span_id":"1a9978d508c86b","name":"middleware","start_time":1549128157239,"finish_time":1549128157339,"category":"generic","tags":{},"parent_id":"fae87301e545a8"},{"trace_id":"fae87301e545a8","span_id":"f05d50d7bef388","name":"sender","start_time":1549128157341,"finish_time":1549128157346,"category":"generic","tags":{},"parent_id":"fae87301e545a8"},{"trace_id":"fae87301e545a8","span_id":"fae87301e545a8","name":"/external","start_time":1549128157237,"finish_time":1549128157348,"category":"generic","tags":{"http.method":"GET","span.kind":"server","http.url":"/external","http.status_code":304}}]' 'http://localhost:12345/?license_key=d67afc830dab717fd163bfcb0b8b88423e9a1a3b&entity_name=test_tracer'
```
