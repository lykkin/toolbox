#!/bin/bash

# Create default keyspace for single node cluster
KS="CREATE KEYSPACE IF NOT EXISTS span_collector WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
TS="CREATE TABLE IF NOT EXISTS span_collector.span (trace_id text, span_id text, name text, start_time double, finish_time double, PRIMARY KEY(trace_id, span_id));"
until echo $KS | cqlsh && echo $TS | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later"
    sleep 2
done &

exec /docker-entrypoint.sh "$@"
