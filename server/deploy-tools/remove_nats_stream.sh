#!/bin/bash

# Removes the Consumers and Stream for the PIT account.
# Intended to be run as pit_admin.

nats consumer rm policies-event-channel claims_sync_rest_consumer
nats consumer rm policies-event-channel underwriting_sync_rest_consumer
nats stream rm policies-event-channel
