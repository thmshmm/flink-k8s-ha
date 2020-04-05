# Flink Kubernetes HA
An implementation of Flinks *HighAvailabilityServicesFactory* for clusters running on Kubernetes.

**This project is currently work in progress.**

Assumptions made:
* Only one JobManager is running at a time (achieved through K8s 'Recreate' deployment strategy)

To use multiple JobManagers the Kubernetes Leader Election mechanism ([Fabric8 Example](https://github.com/fabric8io/kubernetes-client/blob/master/kubernetes-examples/src/main/java/io/fabric8/kubernetes/examples/LeaderElectionExamples.java)) could be implemented.

Not yet implemented:
* CompletedCheckpointStore (currently only in-memory)
* CheckpointIDCounter (currently only in-memory)

## Testing
The [helm-chart-flink](https://github.com/thmshmm/helm-chart-flink) repo is used for local Flink + S3 deployment.
