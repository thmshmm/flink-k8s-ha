package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KubernetesRunningJobsRegistry implements RunningJobsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesRunningJobsRegistry.class);

    private final Configuration configuration;
    private final KubernetesStateHandler kubernetesStateHandler;

    public KubernetesRunningJobsRegistry(Configuration configuration, KubernetesStateHandler kubernetesStateHandler) {
        this.configuration = configuration;
        this.kubernetesStateHandler = kubernetesStateHandler;
    }

    public void setJobRunning(JobID jobID) throws IOException {
        kubernetesStateHandler.updateJobSchedulingStatus(jobID, JobSchedulingStatus.RUNNING);
    }

    public void setJobFinished(JobID jobID) throws IOException {
        kubernetesStateHandler.updateJobSchedulingStatus(jobID, JobSchedulingStatus.DONE);
    }

    public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
        return kubernetesStateHandler.getJobSchedulingStatus(jobID);
    }

    public void clearJob(JobID jobID) throws IOException {
        kubernetesStateHandler.removeJobState(jobID);
    }
}
