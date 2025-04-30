#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

NAMESPACE="data-feed-collect"
GATEWAY_NODEPORT="31112" # Example NodePort in the 30xxx range

echo "--- Deploying OpenFaaS Community Edition ---"

# 1. Add the OpenFaaS helm repository
echo "Adding OpenFaaS helm repository..."
helm repo add openfaas https://openfaas.github.io/faas-netd/

# 2. Update your local helm repositories
echo "Updating helm repositories..."
helm repo update

# 3. Create the namespace if it doesn't exist
echo "Creating namespace '$NAMESPACE' if it doesn't exist..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# 4. Install OpenFaaS using helm
echo "Installing OpenFaaS chart into namespace '$NAMESPACE'..."
helm upgrade openfaas \
    openfaas/openfaas \
    --install \
    --namespace "$NAMESPACE" \
    --set gateway.serviceType=NodePort \
    --set gateway.nodePort="$GATEWAY_NODEPORT" \
    --set basic_auth=true # Recommended for CE

echo "--- OpenFaaS deployment initiated ---"
echo "Wait for pods to be ready in the '$NAMESPACE' namespace."
echo "You can check the status with: kubectl get pods -n $NAMESPACE"
echo "Once ready, find the NodePort for the gateway service:"
echo "kubectl get svc -n $NAMESPACE openfaas-gateway"
echo "The gateway will be accessible on any cluster node IP at port $GATEWAY_NODEPORT."

# Optional: Get the admin password
echo "To get the admin password, run:"
echo "kubectl get secret -n $NAMESPACE basic-auth -o jsonpath='{.data.basic-auth-password}' | base64 --decode"
