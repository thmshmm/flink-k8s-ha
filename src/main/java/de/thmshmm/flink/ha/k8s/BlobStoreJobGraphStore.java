package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class BlobStoreJobGraphStore implements JobGraphStore {

    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreJobGraphStore.class);

    private final Configuration configuration;
    private final KubernetesStateHandler kubernetesStateHandler;
    private final FileSystem fs;
    private final Path jobGraphStoragePath;

    public BlobStoreJobGraphStore(Configuration configuration, KubernetesStateHandler kubernetesStateHandler) throws IOException {
        this.configuration = configuration;
        this.kubernetesStateHandler = kubernetesStateHandler;
        this.jobGraphStoragePath = new Path(HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "graph");
        this.fs = FileSystem.get(jobGraphStoragePath.toUri());
    }

    public void start(JobGraphListener jobGraphListener) throws Exception {
    }

    public void stop() throws Exception {
    }

    public JobGraph recoverJobGraph(JobID jobId) throws Exception {
        Path filePath = createJobGraphFilePath(jobId);
        JobGraph jobGraph = null;
        if (fs.exists(filePath)) {
            try (FSDataInputStream inStream = fs.open(filePath)) {
                jobGraph = InstantiationUtil.deserializeObject(inStream, ClassLoader.getSystemClassLoader());
            } catch (IOException e) {
                LOG.error("Failed recover JobGraph from FS for job {} with exception {}", jobId.toHexString(), e.getMessage());
                throw e;
            }
        }
        return jobGraph;
    }

    public Collection<JobID> getJobIds() throws Exception {
        return kubernetesStateHandler.getJobIds();
    }

    public void putJobGraph(JobGraph jobGraph) throws Exception {
        LOG.info("Writing JobGraph to FS for job {}", jobGraph.getJobID().toHexString());
        Path filePath = createJobGraphFilePath(jobGraph.getJobID());
        try (FSDataOutputStream outStream = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE)) {
            InstantiationUtil.serializeObject(outStream, jobGraph);
        } catch (IOException e) {
            LOG.error("Failed to write JobGraph to FS for job {} with exception {}", jobGraph.getJobID().toHexString(), e.getMessage());
        }
    }

    public void removeJobGraph(JobID jobId) throws Exception {
        Path filePath = createJobGraphFilePath(jobId);
        try {
            fs.delete(filePath, false);
        } catch (IOException e) {
            LOG.error("Failed to delete JobGraph from FS for job {} with exception {}", jobId.toHexString(), e.getMessage());
        }
    }

    public void releaseJobGraph(JobID jobId) throws Exception {
    }

    private Path createJobGraphFilePath(JobID jobID) {
        return new Path(jobGraphStoragePath, "job_" + jobID.toHexString());
    }
}
