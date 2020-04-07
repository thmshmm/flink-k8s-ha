package de.thmshmm.flink.ha.k8s;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.getJobManagerAddress;
import static org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.getWebMonitorAddress;

public class KubernetesHAServicesFactory implements HighAvailabilityServicesFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesHAServicesFactory.class);

    public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {

        KubernetesStateHandler kubernetesStateHandler = new KubernetesStateHandler(configuration, KubernetesUtils.createClient(configuration));

        BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

        RunningJobsRegistry runningJobsRegistry = new KubernetesRunningJobsRegistry(configuration, kubernetesStateHandler);

        JobGraphStore jobGraphStore = new BlobStoreJobGraphStore(configuration, kubernetesStateHandler);

        final Tuple2<String, Integer> hostnamePort = getJobManagerAddress(configuration);

        final String jobManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                JobMaster.JOB_MANAGER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION,
                configuration);
        final String resourceManagerRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                ResourceManager.RESOURCE_MANAGER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION,
                configuration);
        final String dispatcherRpcUrl = AkkaRpcServiceUtils.getRpcUrl(
                hostnamePort.f0,
                hostnamePort.f1,
                Dispatcher.DISPATCHER_NAME,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION,
                configuration);
        final String webMonitorAddress = getWebMonitorAddress(
                configuration,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

        return new KubernetesHAServices(
                configuration,
                kubernetesStateHandler,
                runningJobsRegistry,
                blobStoreService,
                jobGraphStore,
                jobManagerRpcUrl,
                resourceManagerRpcUrl,
                dispatcherRpcUrl,
                webMonitorAddress
        );
    }
}
