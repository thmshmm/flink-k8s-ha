package de.thmshmm.flink.ha.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class KubernetesStateHandler {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandler.class);

    private final String STATE_SCHEDULING_STATUS_KEY = "schedulingStatus";

    private final Configuration configuration;
    private final KubernetesClient client;
    private final String jobStateConfigMapPrefix;
    private final String clusterId;

    public KubernetesStateHandler(Configuration configuration, KubernetesClient client) {
        this.configuration = configuration;
        this.client = client;
        this.clusterId = configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
        this.jobStateConfigMapPrefix = this.clusterId + "-job-state-";
    }

    public List<JobID> getJobIds() {
        LOG.info("Retrieving all stored job ids from ConfigMap for cluster.id={}", clusterId);
        List<JobID> jobIdList = client.configMaps().list().getItems().stream()
                .map(configMap -> configMap.getMetadata().getName())
                .filter(name -> name.startsWith(jobStateConfigMapPrefix))
                .map(name -> JobID.fromHexString(name.substring(jobStateConfigMapPrefix.length())))
                .collect(Collectors.toList());
        if (LOG.isDebugEnabled()) {
            for (JobID jobId : jobIdList) {
                LOG.debug("Found ConfigMap for job {}", jobId.toHexString());
            }
        }
        return jobIdList;
    }

    public RunningJobsRegistry.JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
        String state = "";
        try {
            state = getJobStateConfigMap(jobID).getData().get(STATE_SCHEDULING_STATUS_KEY);
        } catch (NullPointerException e) {
            createInitialConfigMap(jobID);
            LOG.info("Failed to get job scheduling status for job {}, returning state PENDING", jobID.toHexString());
            return RunningJobsRegistry.JobSchedulingStatus.PENDING;
        }
        return RunningJobsRegistry.JobSchedulingStatus.valueOf(state);
    }

    private void createInitialConfigMap(JobID jobID) throws IOException {
        LOG.info("Creating an initial job state ConfigMap for job {}", jobID.toHexString());
        HashMap<String, String> initialStateData = new HashMap<>();
        initialStateData.put(STATE_SCHEDULING_STATUS_KEY, RunningJobsRegistry.JobSchedulingStatus.PENDING.name());
        ConfigMap configMap = client.configMaps().create(
                new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(getJobConfigMapName(jobID))
                        .endMetadata()
                        .withData(initialStateData)
                        .build()
        );
        if (configMap == null) {
            throw new IOException("Failed to create empty job state ConfigMap for job " + jobID.toHexString());
        }
    }

    private ConfigMap getJobStateConfigMap(JobID jobID) {
        return client.configMaps().withName(getJobConfigMapName(jobID)).get();
    }

    public void updateJobSchedulingStatus(JobID jobID, RunningJobsRegistry.JobSchedulingStatus status) throws IOException {
        if (client.configMaps().withName(getJobConfigMapName(jobID)).edit().addToData(STATE_SCHEDULING_STATUS_KEY, status.name()).done() == null) {
            throw new IOException("Failed to update job scheduling status for job " + jobID.toHexString());
        }
    }

    private String getJobConfigMapName(JobID jobID) {
        return jobStateConfigMapPrefix + jobID.toHexString();
    }

    public void removeJobState(JobID jobID) {
        client.configMaps().withName(getJobConfigMapName(jobID)).delete();
    }
}
