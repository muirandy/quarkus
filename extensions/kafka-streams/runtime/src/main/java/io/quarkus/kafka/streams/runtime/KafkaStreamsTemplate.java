package io.quarkus.kafka.streams.runtime;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.rocksdb.RocksDB;

import io.quarkus.arc.Arc;
import io.quarkus.arc.runtime.BeanContainerListener;
import io.quarkus.runtime.annotations.Template;

@Template
public class KafkaStreamsTemplate {

    private static String hotDeploymentInterceptor;

    public void loadRocksDb() {
        RocksDB.loadLibrary();
    }

    public void configureRuntimeProperties(KafkaStreamsRuntimeConfig runtimeConfig) {
        Arc.container().instance(KafkaStreamsTopologyManager.class).get().setRuntimeConfig(runtimeConfig);
    }

    public BeanContainerListener configure(Properties properties) {
        Properties p = new Properties(properties);
        if (hotDeploymentInterceptor != null) {
            p.setProperty(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG),
                    hotDeploymentInterceptor);
        }
        return container -> {
            KafkaStreamsTopologyManager instance = container.instance(KafkaStreamsTopologyManager.class);
            instance.configure(properties);
        };
    }

    public static void setHotDeploymentInterceptor(String className) {
        KafkaStreamsTemplate.hotDeploymentInterceptor = className;
    }
}
