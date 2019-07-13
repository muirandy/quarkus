package io.quarkus.kafka.streams.deployment;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.quarkus.deployment.devmode.HotReplacementContext;
import io.quarkus.deployment.devmode.HotReplacementSetup;
import io.quarkus.kafka.streams.runtime.KafkaStreamsTemplate;

public class KafkaStreamsHotDeploymentSetup implements HotReplacementSetup {

    private static HotReplacementContext context;

    @Override
    public void setupHotDeployment(HotReplacementContext context) {
        KafkaStreamsHotDeploymentSetup.context = context;
        KafkaStreamsTemplate.setHotDeploymentInterceptor(HotDeploymentInterceptor.class.getName());

    }

    public static class HotDeploymentInterceptor<K, V> implements ConsumerInterceptor<K, V> {

        private static final long TWO_SECONDS = 2000;
        private volatile long nextUpdate;

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
            if (nextUpdate < System.currentTimeMillis()) {
                synchronized (this) {
                    if (nextUpdate < System.currentTimeMillis()) {
                        try {
                            context.doScan(true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        if (context.getDeploymentProblem() != null) {
                            context.getDeploymentProblem().printStackTrace();
                        }

                        // we update at most once every 2s
                        nextUpdate = System.currentTimeMillis() + TWO_SECONDS;
                    }
                }
            }
            return records;
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        }

        @Override
        public void close() {
        }
    }
}
