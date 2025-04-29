#!/bin/bash

# Script to deploy ClickHouse using the Bitnami Helm chart

# Define variables
RELEASE_NAME="clickhouse"
NAMESPACE="data-collect-feed" # Or your desired namespace
CHART_REPO="bitnami"
CHART_NAME="clickhouse"
CHART_VERSION="1.x.x" # Specify a chart version, check https://artifacthub.io/packages/helm/bitnami/clickhouse
VALUES_FILE="k8s/clickhouse-values.yaml"

# Add the Bitnami Helm repository if not already added
echo "Adding Bitnami Helm repository..."
helm repo add "${CHART_REPO}" "https://charts.bitnami.com/bitnami"
helm repo update

# Check if the release already exists
if helm status "${RELEASE_NAME}" -n "${NAMESPACE}" &> /dev/null; then
    echo "Helm release '${RELEASE_NAME}' already exists in namespace '${NAMESPACE}'. Upgrading..."
    # Upgrade the existing release
    helm upgrade "${RELEASE_NAME}" "${CHART_REPO}/${CHART_NAME}" \
        --namespace "${NAMESPACE}" \
        --version "${CHART_VERSION}" \
        -f "${VALUES_FILE}" \
        --wait # Wait for the upgrade to complete
else
    echo "Helm release '${RELEASE_NAME}' not found in namespace '${NAMESPACE}'. Installing..."
    # Install the chart
    helm install "${RELEASE_NAME}" "${CHART_REPO}/${CHART_NAME}" \
        --namespace "${NAMESPACE}" \
        --version "${CHART_VERSION}" \
        -f "${VALUES_FILE}" \
        --wait # Wait for the installation to complete
fi

# Verify the deployment
echo "Verifying ClickHouse deployment..."
kubectl get pods -l app.kubernetes.io/name=clickhouse,app.kubernetes.io/instance=${RELEASE_NAME} -n "${NAMESPACE}"
kubectl get svc -l app.kubernetes.io/name=clickhouse,app.kubernetes.io/instance=${RELEASE_NAME} -n "${NAMESPACE}"

echo "ClickHouse deployment script finished."
echo "Check the services using 'kubectl get svc -n ${NAMESPACE}' to find the NodePorts."
