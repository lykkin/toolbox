#!/bin/bash

KS="CREATE KEYSPACE IF NOT EXISTS span_collector WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
TS="CREATE TABLE IF NOT EXISTS span_collector.span (trace_id text, span_id text, parent_id text, name text, start_time double, finish_time double, category text, tags map<text,text>, license_key text, entity_id text, entity_name text, PRIMARY KEY(trace_id, span_id));"
PS="CREATE TABLE IF NOT EXISTS span_collector.to_process (trace_id text, span_id text, parent_id text, name text, start_time double, finish_time double, category text, tags map<text,text>, license_key text, entity_id text, entity_name text, PRIMARY KEY(trace_id, span_id));"

# Create default keyspace for single node cluster
until echo $KS | cqlsh && echo $TS | cqlsh && echo $PS | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later"
    sleep 2
done &

exec /docker-entrypoint.sh "$@"
