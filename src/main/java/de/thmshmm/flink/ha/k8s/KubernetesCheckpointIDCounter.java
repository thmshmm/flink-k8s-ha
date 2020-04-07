package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import java.util.concurrent.atomic.AtomicLong;

class KubernetesCheckpointIDCounter implements CheckpointIDCounter {

    private final JobID jobID;
    private final KubernetesStateHandler kubernetesStateHandler;

    public KubernetesCheckpointIDCounter(JobID jobID, KubernetesStateHandler kubernetesStateHandler) {
        this.jobID = jobID;
        this.kubernetesStateHandler = kubernetesStateHandler;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {
    }

    @Override
    public long getAndIncrement() throws Exception {
        long counter = kubernetesStateHandler.getCheckpointIDCounter(jobID);
        kubernetesStateHandler.updateCheckpointIDCounter(jobID, counter+1);
        return counter;
    }

    @Override
    public long get() {
        return kubernetesStateHandler.getCheckpointIDCounter(jobID);
    }

    @Override
    public void setCount(long newId) throws Exception {
        kubernetesStateHandler.updateCheckpointIDCounter(jobID, newId);
    }
}
