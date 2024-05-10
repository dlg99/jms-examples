package org.example;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdminException;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.Consumer;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.ConsumerStats;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.Message;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.MessageId;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.Producer;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.PulsarClientException;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.SubscriptionMode;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.api.SubscriptionType;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableMap;

import javax.jms.JMSContext;
import javax.jms.Topic;
import java.io.Console;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PulsarAckHolesTest {
    public static void main(String[] args) throws Exception {

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("batchingEnabled", false);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(
                ImmutableMap.of("jms.useServerSideFiltering", "true", "jms.clientId", "test", "producerConfig", producerConfig ));) {

            Topic topic1;
            try (JMSContext jmsContext = factory.createContext()) {
                topic1 = jmsContext.createTopic("mytopic");

                factory.getPulsarAdmin().namespaces().setBacklogQuota("public/default", BacklogQuota
                        .builder()
                        .limitSize(-1)
                        .limitTime(-1)
                        .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold)
                        .build());
                try {
                    factory.getPulsarAdmin().topics().delete(topic1.getTopicName());
                } catch (PulsarAdminException.NotFoundException notFoundException) {

                }
            }

            final int numSubs = 3; // number of subscriptions
            final int numConsumers = 3; // per sub, shared subscription
            final int numMessagesPerSub = 20_000_000;
            final int numMessages = numConsumers * numMessagesPerSub; // messages to produce

            final int batchSize = 100;

            List<Consumer<byte[]>> consumers = new ArrayList<>();
            List<AtomicInteger> ackTracker = new ArrayList<>();
            try {
                for (int n = 0; n < numSubs; n++) {
                    String subName = "sub" + n;
                    factory.getPulsarAdmin().topics()
                            .createSubscription(topic1.getTopicName(), subName, MessageId.earliest, false);

                    for (int i = 0; i < numConsumers; i++) {
                        consumers.add(factory.getPulsarClient()
                                .newConsumer()
                                .topic(topic1.getTopicName())
                                .consumerName("consumer-" + subName + "-" + i)
                                .subscriptionName(subName)
                                .ackTimeout(1000, TimeUnit.MILLISECONDS)
                                .enableRetry(false)
                                .subscriptionMode(SubscriptionMode.Durable)
                                .subscriptionType(SubscriptionType.Shared)
                                .subscribe());
                        ackTracker.add(new AtomicInteger());
                    }
                }

                try(Producer<byte[]> producer = factory.getPulsarClient().newProducer().topic(topic1.getTopicName()).create()) {

                    // produce some messages to have them ready for consumption
                    CompletableFuture<MessageId> lastMessageId = null;
                    for (int i = 0; i < Math.min(numMessages, consumers.size() * batchSize); i++) {
                        lastMessageId = producer.sendAsync(("message-" + i).getBytes());
                        if (i % 10 == 0) {
                            lastMessageId.get(30, TimeUnit.SECONDS);
                        }
                    }
                    lastMessageId.get(30, TimeUnit.SECONDS);

                    List<CompletableFuture<Void>> messages = new LinkedList<>();
                    for (int i = 0; i < numMessages; i++) {
                        // produce one more
                        lastMessageId = producer.sendAsync(("message-" + i).getBytes());
                        final int msgNum = i;

                        // consume a message per consumer, ack every second
                        int consId = 0;
                        for (Consumer<byte[]> cons : consumers) {
                            final int consumerId = consId++;
                            final CompletableFuture<Void> lastMsg = cons
                                    .receiveAsync()
                                    .exceptionally(e -> {
                                                System.err.println(e);
                                                return null;
                                            })
                                    .thenCompose(msg -> {
                                        if (msg == null) {
                                            System.out.println("Consumer " + cons.getConsumerName() + " received null for message " + msgNum);
                                            return CompletableFuture.completedFuture(null);
                                        }
                                        if ((msgNum + consumerId % numSubs) % 2 == 0) {
                                            ackTracker.get(consumerId).incrementAndGet();
                                            //System.out.println("Consumer " + cons.getConsumerName() + " acked message " + msgNum);
                                            return cons.acknowledgeAsync(msg);
                                        }
                                        //cons.negativeAcknowledge(msg);
                                        return CompletableFuture.completedFuture(null);
                                    });

                            messages.add(lastMsg);
                        }

                        // wait to not run out of memory
                        if (messages.size() >= batchSize * consumers.size()) {
                            lastMessageId.get(30, TimeUnit.SECONDS);
                            while (messages.size() > consumers.size()) {
                                messages.remove(0).get(30, TimeUnit.SECONDS);
                            }
                        }

                        if ((i + 1) % 50000 == 0) {
                            printConsumersProgress(consumers, ackTracker, (i + 1));
                        }

                    }
                    while (!messages.isEmpty()) {
                        messages.remove(0).get(30, TimeUnit.SECONDS);
                    }
                    printConsumersProgress(consumers, ackTracker, numMessages);
                }
            } finally {
                printConsumersProgress(consumers, ackTracker, numMessages);
                consumers.forEach(c -> {
                    try {
                        c.close();
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    private static void printConsumersProgress(List<Consumer<byte[]>> consumers, List<AtomicInteger> ackTracker, int i) {
        int consumerId = 0;
        for (Consumer<byte[]> cons : consumers) {
            int numAcked = ackTracker.get(consumerId++).get();
            System.out.println("Consumer " + cons.getConsumerName()
                    + " acked " + numAcked + " of " + i);
        }
    }

}
