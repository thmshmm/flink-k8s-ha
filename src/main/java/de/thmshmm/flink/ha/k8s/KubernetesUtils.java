package de.thmshmm.flink.ha.k8s;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.configuration.ConfigOptions.key;

public class KubernetesUtils {

    public static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

    public static final ConfigOption<String> HA_KUBERNETES_NAMESPACE = key("high-availability.kubernetes.namespace")
            .stringType()
            .noDefaultValue()
            .withDescription("Defines the Kubernetes namespace used for storing cluster metadata.");

    public static KubernetesClient createClient(Configuration configuration) {
        LOG.info("Creating a Kubernetes client for namespace {}", configuration.getString(HA_KUBERNETES_NAMESPACE));
        return new DefaultKubernetesClient().inNamespace(configuration.getString(HA_KUBERNETES_NAMESPACE));
    }
}
