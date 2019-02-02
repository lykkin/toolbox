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

|     Param     |  Type  | Description |
|---------------|--------|-------------|
| `entity_name` | string | Name of the entity the trace was produced from.  |
| `license_key` | string | License key to associate the trace data with. |
|  `entity_id`  | string | ID of the entity the trace was produced from.  |

### Payload
The payload should be JSON with the following fields defined:

|Field|Field Type|Description|
|-----|----------|-----------|
|`span_id`|string|Unique identifier for this span|
|`trace_id`|string|Unique identifier shared by all spans within a single trace|
|`name`|string|The name of this span|
|`parent_id`|string|The span id of the previous caller of this span.|
|`start_time`|float|Timestamp for the start of this span in milliseconds|
|`finish_time`|float|Timestamp for the end of this span in milliseconds|
|`tags`|map<string,*>|Map of user specified "tags" on this span. Keys are strings, values can take any form|
