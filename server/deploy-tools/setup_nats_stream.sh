#!/bin/bash

# Create the Stream and Consumers in the PIT account.
# Intended to be run as pit_admin.

# Create stream.
nats stream add --subjects="policies-event-channel" --storage=file --replicas=#{MSG_QUEUE_REPLICAS}# --retention=interest --discard=new --defaults policies-event-channel

# Create Consumers.
nats consumer add --ack=explicit --deliver=all --pull --backoff=linear --backoff-steps=20 --backoff-min=5s --backoff-max=60m --max-deliver=150 --max-pending=1000 --defaults policies-event-channel claims_sync_rest_consumer

nats consumer add --ack=explicit --deliver=all --pull --backoff=linear --backoff-steps=20 --backoff-min=5s --backoff-max=60m --max-deliver=150 --max-pending=1000 --defaults policies-event-channel underwriting_sync_rest_consumer
