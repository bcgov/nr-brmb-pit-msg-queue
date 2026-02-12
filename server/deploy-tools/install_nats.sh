#!/bin/bash

# Install NATS v2.12.2 using the official helm chart.

# Using Helm version 3. I tried Version 4, but the install fails without explanation.
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh

# Install NATS Helm Repository
helm repo add nats https://nats-io.github.io/k8s/helm/charts/

# Install NATS. This pulls the latest chart, but we should probably choose the 
# tag for v2.12.2.
# Note: You need to have logged in via oc first.
helm install -f override-nats-values.yaml nats-#{ENV}# nats/nats -n #{NAMESPACE}#

