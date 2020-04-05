package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkState;

public class KubernetesHAServices implements HighAvailabilityServices {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesHAServices.class);

    protected final Object lock = new Object();

    private boolean shutdown;

    /**
     * The runtime configuration.
     */
    private final Configuration configuration;

    /**
     * The Kubernetes based running jobs registry.
     */
    private final RunningJobsRegistry runningJobsRegistry;

    /**
     * Store for arbitrary blobs.
     */
    private final BlobStoreService blobStoreService;

    /**
     * Store for job graphs.
     */
    private final JobGraphStore jobGraphStore;

    private final String jobManagerRpcUrl;
    private final String resourceManagerRpcUrl;
    private final String dispatcherRpcUrl;
    private final String webMonitorAddress;

    public KubernetesHAServices(Configuration configuration, RunningJobsRegistry runningJobsRegistry, BlobStoreService blobStoreService, JobGraphStore jobGraphStore, String jobManagerRpcUrl, String resourceManagerRpcUrl, String dispatcherRpcUrl, String webMonitorAddress) {
        this.configuration = configuration;
        this.runningJobsRegistry = runningJobsRegistry;
        this.blobStoreService = blobStoreService;
        this.jobManagerRpcUrl = jobManagerRpcUrl;
        this.resourceManagerRpcUrl = resourceManagerRpcUrl;
        this.dispatcherRpcUrl = dispatcherRpcUrl;
        this.webMonitorAddress = webMonitorAddress;
        this.jobGraphStore = jobGraphStore;
        this.shutdown = false;
    }

    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
        return new CheckpointRecoveryFactory() {
            @Override
            public CompletedCheckpointStore createCheckpointStore(JobID jobId, int maxNumberOfCheckpointsToRetain, ClassLoader userClassLoader) throws Exception {
                return new CompletedCheckpointStore() {
                    private List<CompletedCheckpoint> checkpoints = new ArrayList<>();

                    @Override
                    public void recover() throws Exception {

                    }

                    @Override
                    public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
                        checkpoints.add(checkpoint);
                    }

                    @Override
                    public void shutdown(JobStatus jobStatus) throws Exception {

                    }

                    @Override
                    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
                        return checkpoints;
                    }

                    @Override
                    public int getNumberOfRetainedCheckpoints() {
                        return checkpoints.size();
                    }

                    @Override
                    public int getMaxNumberOfRetainedCheckpoints() {
                        return 10;
                    }

                    @Override
                    public boolean requiresExternalizedCheckpoints() {
                        return true;
                    }
                };
            }

            @Override
            public CheckpointIDCounter createCheckpointIDCounter(JobID jobId) throws Exception {
                return new CheckpointIDCounter() {
                    AtomicLong counter = new AtomicLong(0);

                    @Override
                    public void start() throws Exception {

                    }

                    @Override
                    public void shutdown(JobStatus jobStatus) throws Exception {

                    }

                    @Override
                    public long getAndIncrement() throws Exception {
                        return counter.getAndIncrement();
                    }

                    @Override
                    public long get() {
                        return counter.get();
                    }

                    @Override
                    public void setCount(long newId) throws Exception {
                        counter.set(newId);
                    }
                };
            }
        };
    }

    public JobGraphStore getJobGraphStore() throws Exception {
        return jobGraphStore;
    }

    public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
        return runningJobsRegistry;
    }

    public BlobStore createBlobStore() throws IOException {
        return blobStoreService;
    }

    public void close() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                if (blobStoreService != null) blobStoreService.close();
                shutdown = true;
            }
        }
    }

    public void closeAndCleanupAllData() throws Exception {
    }

    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderRetrievalService(resourceManagerRpcUrl, DEFAULT_LEADER_ID);
        }
    }

    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderRetrievalService(dispatcherRpcUrl, DEFAULT_LEADER_ID);
        }
    }

    public LeaderElectionService getResourceManagerLeaderElectionService() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderElectionService();
        }
    }

    public LeaderElectionService getDispatcherLeaderElectionService() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderElectionService();
        }
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderRetrievalService(jobManagerRpcUrl, DEFAULT_LEADER_ID);
        }
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderRetrievalService(defaultJobManagerAddress, DEFAULT_LEADER_ID);
        }
    }

    public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderElectionService();
        }
    }

    public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderRetrievalService(webMonitorAddress, DEFAULT_LEADER_ID);
        }
    }

    public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
        synchronized (lock) {
            checkNotShutdown();
            return new StandaloneLeaderElectionService();
        }
    }

    @GuardedBy("lock")
    protected void checkNotShutdown() {
        checkState(!shutdown, "high availability services are shut down");
    }
}
