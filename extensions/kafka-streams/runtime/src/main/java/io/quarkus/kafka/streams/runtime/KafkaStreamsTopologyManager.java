package io.quarkus.kafka.streams.runtime;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.jboss.logging.Logger;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

/**
 * Manages the lifecycle of a Kafka Streams pipeline. If there's a producer
 * method returning a KS {@link Topology}, then this topology will be configured
 * and started. Optionally, before starting the pipeline, this manager will wait
 * for a given set of topics to be created, as KS itself will fail without all
 * input topics being created upfront.
 */
@ApplicationScoped
public class KafkaStreamsTopologyManager {

    private static final String STREAMS_OPTION_PREFIX = "kafka-streams.";

    private static final Logger LOGGER = Logger.getLogger(KafkaStreamsTopologyManager.class.getName());

    private final ExecutorService executor;
    private KafkaStreams streams;
    private KafkaStreamsRuntimeConfig runtimeConfig;
    private Instance<Topology> topology;
    private Properties properties;

    KafkaStreamsTopologyManager() {
        executor = null;
    }

    @Inject
    public KafkaStreamsTopologyManager(Instance<Topology> topology) {
        // No producer for Topology -> nothing to do
        if (topology.isUnsatisfied()) {
            executor = null;

            return;
        }
        System.out.println("#### Topo" + topology);
        executor = Executors.newSingleThreadExecutor();

        this.topology = topology;
    }

    /**
     * Returns all properties to be passed to Kafka Streams.
     */
    private static Properties getStreamsProperties(Properties properties, KafkaStreamsRuntimeConfig runtimeConfig) {
        // build-time options
        Map<Object, Object> filtered = properties.entrySet()
                .stream()
                .filter(e -> ((String) e.getKey()).startsWith(STREAMS_OPTION_PREFIX))
                .collect(
                        Collectors.toMap(
                                e -> ((String) e.getKey()).substring(STREAMS_OPTION_PREFIX.length()),
                                e -> (String) e.getValue()));

        // add runtime options
        filtered.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, runtimeConfig.bootstrapServers);
        filtered.put(StreamsConfig.APPLICATION_ID_CONFIG, runtimeConfig.applicationId);

        if (runtimeConfig.applicationServer.isPresent()) {
            filtered.put(StreamsConfig.APPLICATION_SERVER_CONFIG, runtimeConfig.applicationServer.get());
        }

        Properties streamsProperties = new Properties();
        streamsProperties.putAll(filtered);

        return streamsProperties;
    }

    void onStart(@Observes StartupEvent ev) {
        Properties streamsProperties = getStreamsProperties(properties, runtimeConfig);

        Set<String> topicsToAwait = runtimeConfig.topics
                .map(n -> n.split(","))
                .map(Arrays::asList)
                .map(HashSet::new)
                .map(Collections::unmodifiableSet)
                .orElseGet(Collections::emptySet);

        LOGGER.error("#### topo" + topology);
        streams = new KafkaStreams(topology.get(), streamsProperties);

        executor.execute(() -> {
            try {
                waitForTopicsToBeCreated(topicsToAwait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            LOGGER.debug("Starting Kafka Streams pipeline");
            streams.start();
        });
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (streams != null) {
            LOGGER.debug("Stopping Kafka Streams pipeline");
            streams.close();
        }
    }

    @Produces
    @Singleton
    public KafkaStreams getStreams() {
        return streams;
    }

    private void waitForTopicsToBeCreated(Set<String> topicsToAwait) throws InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, runtimeConfig.bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            while (true) {
                try {
                    ListTopicsResult topics = adminClient.listTopics();
                    Set<String> topicNames = topics.names().get(10, TimeUnit.SECONDS);

                    if (topicNames.containsAll(topicsToAwait)) {
                        LOGGER.debug("All expected topics created");
                        return;
                    } else {
                        HashSet<String> missing = new HashSet<>(topicsToAwait);
                        missing.retainAll(topicNames);
                        LOGGER.debug("Waiting for topic(s) to be created: " + missing);
                    }

                    Thread.sleep(1_000);
                } catch (ExecutionException | TimeoutException e) {
                    LOGGER.error("Failed to get topic names from broker", e);
                }
            }
        }
    }

    public void setRuntimeConfig(KafkaStreamsRuntimeConfig runtimeConfig) {
        this.runtimeConfig = runtimeConfig;
    }

    public void configure(Properties properties) {
        this.properties = properties;
    }
}
