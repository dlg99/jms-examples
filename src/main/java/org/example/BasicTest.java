package org.example;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdminException;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.BacklogQuota;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableMap;

public class BasicTest {


    public static void main(String[] args) throws Exception {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("batchingEnabled", false);
        Map<String, Object> configuration = new HashMap<>(
                ImmutableMap.of("jms.useServerSideFiltering", "true", "jms.clientId", "test", "producerConfig",
                        producerConfig, "jms.emulateTransactions", true
                ));
        if (args.length > 0) {
            System.out.println("Reading configuration from JSON file " + args[0]);
            Map<String, Object> read = new ObjectMapper().readValue(new File(args[0]), Map.class);
            configuration.putAll(read);
        }

        String topic = (String) configuration.remove("topic");
        if (topic == null) {
            throw new RuntimeException("please set topic=xx in the configuration file");
        }
        System.out.println("Topic is " + topic);

        System.out.println("PulsarConnectionFactory Configuration: " + configuration);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(configuration);) {

            Topic topic1;
            try (JMSContext jmsContext = factory.createContext()) {
                topic1 = jmsContext.createTopic(topic);
            }

            Histogram histogram = new Histogram(new ExponentiallyDecayingReservoir());

            long startTime = System.currentTimeMillis();
            try (JMSContext jmsContext = factory.createContext()) {
                long lastTime = System.currentTimeMillis();
                long count = 0;
                while (true) {
                    long now = System.nanoTime();
                    JMSProducer producer = jmsContext.createProducer();
                    producer.send(topic1, "text");
                    histogram.update(System.nanoTime() - now);
                    count++;

                    long nowMillis = System.currentTimeMillis();
                    if (nowMillis - lastTime > 3000) {
                        long delta = nowMillis - lastTime;
                        long throughout = (long) (count / (delta / 1000.0));
                        lastTime = nowMillis;
                        System.out.println(
                                Instant.now() + " " + count + " messages (" + ((nowMillis - startTime) / 1000)
                                        + " s since start), "
                                        + "p50: " + (int) (histogram.getSnapshot().getMean() / 1000000) + " ms, "
                                        + "p75: " + (int) (histogram.getSnapshot().get75thPercentile() / 1000000)
                                        + " ms, "
                                        + "p99: " + (int) (histogram.getSnapshot().get99thPercentile() / 1000000)
                                        + " ms, "
                                        + "throughout: " + throughout + " msg/s");
                        count = 0;
                    }
                }
            }


        }
    }

}