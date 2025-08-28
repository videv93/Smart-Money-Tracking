# Smart Money Tracking Helm Chart

A comprehensive Helm chart for deploying the Smart Money Tracking MLOps pipeline on Kubernetes.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.8+
- PV provisioner support in the underlying infrastructure
- StorageClass configured for persistent volumes

## Architecture

This Helm chart deploys a complete MLOps pipeline for blockchain smart money tracking including:

### Core Components
- **Blockchain Ingestion**: Multi-chain data collection (Ethereum, Polygon, Arbitrum)
- **Batch Processing**: Spark-based data processing with scheduled jobs
- **Stream Processing**: Real-time data processing with Flink
- **ML Model**: Smart money classification model with training pipeline
- **Monitoring**: Custom alerts and metrics collection

### Data Infrastructure
- **PostgreSQL**: Data warehouse with staging and production schemas
- **MinIO**: S3-compatible object storage for Delta Lake
- **Kafka**: Message streaming for real-time data pipeline
- **Redis**: Cache and message broker for Airflow

### Monitoring & Orchestration
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Visualization and dashboards
- **Airflow**: Workflow orchestration and scheduling

## Installation

### Add the Bitnami repository (for dependencies)
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

### Install the chart
```bash
# Create namespace
kubectl create namespace smart-money-tracking

# Install with default values
helm install smart-money-tracking ./helm/smart-money-tracking \
  --namespace smart-money-tracking

# Install with custom values
helm install smart-money-tracking ./helm/smart-money-tracking \
  --namespace smart-money-tracking \
  --values custom-values.yaml
```

## Configuration

### Required Configuration

Before installing, you must configure the following values:

```yaml
secrets:
  blockchain:
    ethereumRpcUrl: "https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
    polygonRpcUrl: "https://polygon-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
    arbitrumRpcUrl: "https://arb-mainnet.g.alchemy.com/v2/YOUR_API_KEY"
```

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global.storageClass` | Storage class for persistent volumes | `""` |
| `components.blockchainIngestion.enabled` | Enable blockchain data ingestion | `true` |
| `components.batchProcessing.enabled` | Enable batch processing jobs | `true` |
| `components.batchProcessing.schedule` | Cron schedule for batch jobs | `"0 */6 * * *"` |
| `components.streamProcessing.enabled` | Enable stream processing | `true` |
| `components.mlModel.enabled` | Enable ML model component | `true` |
| `postgresql.enabled` | Enable PostgreSQL dependency | `true` |
| `minio.enabled` | Enable MinIO dependency | `true` |
| `kafka.enabled` | Enable Kafka dependency | `true` |
| `monitoring.prometheus.enabled` | Enable Prometheus monitoring | `true` |
| `monitoring.grafana.enabled` | Enable Grafana dashboards | `true` |
| `airflow.enabled` | Enable Airflow orchestration | `true` |

### Persistence Configuration

```yaml
persistence:
  enabled: true
  storageClass: "fast-ssd"  # Use your storage class
  size:
    data: 100Gi
    logs: 20Gi
    checkpoints: 10Gi
```

### Resource Configuration

```yaml
components:
  blockchainIngestion:
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"
      limits:
        memory: "1Gi"
        cpu: "500m"
  
  batchProcessing:
    resources:
      requests:
        memory: "2Gi"
        cpu: "1000m"
      limits:
        memory: "4Gi"
        cpu: "2000m"
```

### Monitoring and Alerting

```yaml
secrets:
  slack:
    webhookUrl: "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
  email:
    smtpHost: "smtp.gmail.com"
    smtpPort: 587
    username: "your-email@gmail.com"
    password: "your-app-password"

config:
  monitoring:
    alertThresholds:
      largeTransaction: 100  # ETH
      unusualGas: 500       # Gwei
      smartMoneyConfidence: 0.9
```

## Usage

### Accessing Services

#### Blockchain Ingestion API
```bash
kubectl port-forward svc/smart-money-tracking-blockchain-ingestion 8080:80
```

#### Airflow Web UI
```bash
kubectl port-forward svc/smart-money-tracking-airflow-webserver 8080:8080
# Access: http://localhost:8080
```

#### Grafana Dashboard
```bash
kubectl port-forward svc/smart-money-tracking-grafana 3000:80
# Access: http://localhost:3000
# Default: admin / admin123
```

#### PostgreSQL Database
```bash
kubectl exec -it deployment/smart-money-tracking-postgresql -- psql -U smartmoney -d smart_money_db
```

### Scaling Components

```bash
# Scale blockchain ingestion
kubectl scale deployment smart-money-tracking-blockchain-ingestion --replicas=3

# Scale stream processing
kubectl scale deployment smart-money-tracking-stream-processing --replicas=2

# Scale Airflow workers
kubectl scale deployment smart-money-tracking-airflow-worker --replicas=4
```

### Monitoring

```bash
# Check all pods
kubectl get pods -n smart-money-tracking

# View logs
kubectl logs -f deployment/smart-money-tracking-blockchain-ingestion
kubectl logs -f deployment/smart-money-tracking-stream-processing

# Check batch job status
kubectl get cronjobs
kubectl get jobs
```

## Upgrading

```bash
# Upgrade with new values
helm upgrade smart-money-tracking ./helm/smart-money-tracking \
  --namespace smart-money-tracking \
  --values updated-values.yaml

# Check upgrade status
helm history smart-money-tracking -n smart-money-tracking
```

## Troubleshooting

### Common Issues

1. **Pods stuck in Pending state**
   - Check PVC status: `kubectl get pvc`
   - Verify StorageClass: `kubectl get storageclass`
   - Check node resources: `kubectl describe nodes`

2. **Database connection issues**
   - Verify PostgreSQL is running: `kubectl get pods | grep postgresql`
   - Check service connectivity: `kubectl get svc`
   - Review database logs: `kubectl logs deployment/smart-money-tracking-postgresql`

3. **Kafka connectivity issues**
   - Check Kafka pods: `kubectl get pods | grep kafka`
   - Verify topics creation: `kubectl exec -it kafka-pod -- kafka-topics.sh --list --bootstrap-server localhost:9092`

4. **Memory/CPU limits**
   - Check resource usage: `kubectl top pods`
   - Adjust resource limits in values.yaml
   - Monitor with: `kubectl describe pod POD_NAME`

### Debugging Commands

```bash
# Get all resources
kubectl get all -n smart-money-tracking

# Describe problematic pod
kubectl describe pod POD_NAME

# Check events
kubectl get events --sort-by='.metadata.creationTimestamp'

# Check persistent volumes
kubectl get pv,pvc

# Test database connectivity
kubectl run -it --rm debug --image=postgres:14 --restart=Never -- psql -h smart-money-tracking-postgresql -U smartmoney -d smart_money_db
```

## Uninstalling

```bash
# Uninstall the chart
helm uninstall smart-money-tracking -n smart-money-tracking

# Clean up persistent volumes (if needed)
kubectl delete pvc --all -n smart-money-tracking

# Delete namespace
kubectl delete namespace smart-money-tracking
```

## Development

### Building Custom Images

```bash
# Build blockchain ingestion
docker build -t your-registry/smart-money-tracking/blockchain-ingestion:latest ./blockchain_ingestion/

# Build batch processing
docker build -t your-registry/smart-money-tracking/batch-processing:latest ./batch_processing/

# Build stream processing  
docker build -t your-registry/smart-money-tracking/stream-processing:latest ./stream_processing/

# Build ML model
docker build -t your-registry/smart-money-tracking/ml-model:latest ./model_experiment/

# Build monitoring
docker build -t your-registry/smart-money-tracking/monitoring:latest ./monitoring/
```

### Testing

```bash
# Lint the chart
helm lint ./helm/smart-money-tracking

# Dry run installation
helm install smart-money-tracking ./helm/smart-money-tracking \
  --dry-run --debug

# Template rendering
helm template smart-money-tracking ./helm/smart-money-tracking \
  --values test-values.yaml
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For support and questions:
- Create an issue in the GitHub repository
- Check the troubleshooting section above
- Review Kubernetes and Helm documentation