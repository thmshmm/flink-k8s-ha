package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.*;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

class FSCompletedCheckpointStore implements CompletedCheckpointStore {

    private static final Logger LOG = LoggerFactory.getLogger(FSCompletedCheckpointStore.class);

    private static Comparator<CompletedCheckpoint> checkpointComparator = Comparator.comparing(CompletedCheckpoint::getCheckpointID);

    protected final Object lock = new Object();

    private final FileSystem fs;

    private JobID jobID;
    private ClassLoader userClassLoader;
    private final int maxNumberOfCheckpointsToRetain;
    private final ArrayDeque<CompletedCheckpoint> completedCheckpoints;
    private final Path storagePath;

    private boolean shutdown = false;

    public FSCompletedCheckpointStore(Configuration configuration, JobID jobID, ClassLoader userClassLoader, int maxNumberOfCheckpointsToRetain) throws IOException {
        this.jobID = jobID;
        this.userClassLoader = userClassLoader;
        this.maxNumberOfCheckpointsToRetain = maxNumberOfCheckpointsToRetain;
        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfCheckpointsToRetain + 1);
        this.storagePath = new Path(HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "checkpoint");
        this.fs = FileSystem.get(storagePath.toUri());
    }

    @Override
    public void recover() throws Exception {
        Path filePath = createJobFilePath(jobID);

        FileStatus[] outStream = fs.listStatus(filePath);

        ArrayList<CompletedCheckpoint> recoveredCheckpoints = new ArrayList<>();
        for (FileStatus status : outStream) {
            try (FSDataInputStream inStream = fs.open(status.getPath())) {
                CompletedCheckpoint checkpoint = InstantiationUtil.deserializeObject(inStream, userClassLoader);
                recoveredCheckpoints.add(checkpoint);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Successfully restored CompletedCheckpoint {} for job {}", checkpoint.getCheckpointID(), checkpoint.getJobId().toHexString());
                }
            } catch (IOException e) {
                LOG.error("Failed recover CompletedCheckpoint from FS for job {} with exception {}", jobID.toHexString(), e.getMessage());
                throw e;
            }
        }

        Collections.sort(recoveredCheckpoints, checkpointComparator);

        recoveredCheckpoints.subList(
                recoveredCheckpoints.size() < maxNumberOfCheckpointsToRetain ? 0
                        : recoveredCheckpoints.size() - maxNumberOfCheckpointsToRetain, recoveredCheckpoints.size()
        ).forEach(completedCheckpoints::offer);

        LOG.info("Successfully restored {} CompletedCheckpoints for job {}", completedCheckpoints.size(), jobID.toHexString());
    }

    @Override
    public void addCheckpoint(CompletedCheckpoint checkpoint) throws Exception {
        LOG.info("Writing CompletedCheckpoint {} to FS for job {}", checkpoint.getCheckpointID(), checkpoint.getJobId().toHexString());

        Path filePath = createCheckpointFilePath(checkpoint.getJobId(), checkpoint.getCheckpointID());

        try (FSDataOutputStream outStream = fs.create(filePath, FileSystem.WriteMode.NO_OVERWRITE)) {
            InstantiationUtil.serializeObject(outStream, checkpoint);
        } catch (IOException e) {
            LOG.error("Failed to write CompletedCheckpoint {} to FS for job {} with exception {}", checkpoint.getCheckpointID(), checkpoint.getJobId().toHexString(), e.getMessage());
        }

        completedCheckpoints.offer(checkpoint);

        if (completedCheckpoints.size() > maxNumberOfCheckpointsToRetain) {
            CompletedCheckpoint oldCheckpoint = completedCheckpoints.removeFirst();
            removeCheckpointFromFS(oldCheckpoint);
            oldCheckpoint.discardOnSubsume();
        }

        LOG.info("Successfully added CompletedCheckpoint {} for job {}", checkpoint.getCheckpointID(), checkpoint.getJobId().toHexString());
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() throws Exception {
        return new ArrayList<>(completedCheckpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfCheckpointsToRetain;
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return true;
    }

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {
        synchronized (lock) {
            checkNotShutdown();
            shutdown = true;
            if (jobStatus.isGloballyTerminalState()) {
                LOG.info("Shutting down");
                for (CompletedCheckpoint checkpoint : completedCheckpoints) {
                    removeCheckpointFromFS(checkpoint);
                    checkpoint.discardOnShutdown(jobStatus);
                }
            } else {
                LOG.info("Suspending");
            }
            completedCheckpoints.clear();
        }
    }

    @GuardedBy("lock")
    protected void checkNotShutdown() {
        checkState(!shutdown, "high availability services are shut down");
    }

    private Path createJobFilePath(JobID jobID) {
        return new Path(storagePath, "job_" + jobID.toHexString());
    }

    private Path createCheckpointFilePath(JobID jobID, long checkpointID) {
        return new Path(createJobFilePath(jobID), new Path("chk-" + checkpointID));
    }

    private void removeCheckpointFromFS(CompletedCheckpoint checkpoint) {
        Path filePath = createCheckpointFilePath(checkpoint.getJobId(), checkpoint.getCheckpointID());
        try {
            fs.delete(filePath, false);
        } catch (IOException e) {
            LOG.warn("Failed to delete CompletedCheckpoint {} to FS for job {} with exception {}", checkpoint.getCheckpointID(), checkpoint.getJobId().toHexString(), e.getMessage());
        }
    }
}
