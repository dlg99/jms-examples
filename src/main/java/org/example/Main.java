package org.example;

import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdminException;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.BacklogQuota;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.TopicStats;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import javax.jms.CompletionListener;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Topic;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableMap;


public class Main
{

    private static final Map<String, String> SELECTORS = new HashMap<String, String>() {
        {
            put("sub1", "param < 1");
            put("sub10", "param < 10");
            put("sub50", "param < 50");
            put("sub100", "param < 100");
        }
    };

    public static void main(String[] args) throws Exception
    {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("batchingEnabled", false);
        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(
        ImmutableMap.of("jms.useServerSideFiltering", "true", "jms.clientId", "test", "producerConfig", producerConfig ));)
        {

            Topic topic1;
            try (JMSContext jmsContext = factory.createContext())
            {
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
            createSubscription(factory, topic1, "sub1");
            createSubscription(factory, topic1, "sub10");
            createSubscription(factory, topic1, "sub50");
            createSubscription(factory, topic1, "sub100");

            consume(factory, topic1, "sub1", 1);
            consume(factory, topic1, "sub10", 1);
            consume(factory, topic1, "sub50", 1);
            consume(factory, topic1, "sub100", 1);

            dumpStats(factory, topic1);


            int numberMessages = 100_000_000;
            // generate messages
            Random random = new Random(1431);
            try (JMSContext jmsContext = factory.createContext())
            {


                System.out.println("Sending " + numberMessages + " messages");
                List<CompletableFuture<?>> handles  = new ArrayList<>();
                for (int i = 0; i < numberMessages; i++)
                {
                    int param = random.nextInt(100);
                    JMSProducer producer = jmsContext.createProducer();
                    CompletableFuture<?> handle = new CompletableFuture<>();
                    handles.add(handle);
                    producer.setAsync(new CompletionListener() {
                        @Override
                        public void onCompletion(Message message) {
                            handle.complete(null);
                        }

                        @Override
                        public void onException(Message message, Exception e) {
                            handle.completeExceptionally(e);
                        }
                    });
                    producer.setProperty("param", param).send(topic1, "text-" + param);
                    //System.out.println("Sent message " + i+" with param="+param);
                    if (handles.size() == 10000) {
                        handles.forEach(CompletableFuture::join);
                        handles.clear();
                        System.out.println("Sent " + i + " messages (" + i *  100 /numberMessages + "%)");
                    }
                }
                handles.forEach(CompletableFuture::join);
                System.out.println("Sent " + numberMessages + " messages");
            }


            dumpStats(factory, topic1);

            System.out.println("***************************************************************************");
            Thread.sleep(5000);

            dumpStats(factory, topic1);

            // let the system "see" the behavior of the subscriptions

            consume(factory, topic1, "sub1", 20);
            consume(factory, topic1, "sub10", 20);
            consume(factory, topic1, "sub50", 20);
            consume(factory, topic1, "sub100", 20);

            // dump again

            dumpStats(factory, topic1);

            System.out.println("***************************************************************************");
            Thread.sleep(5000);

            dumpStats(factory, topic1);

        }
    }


    private static void createSubscription(PulsarConnectionFactory factory, Topic topic, String name) throws Exception
    {
        String selector = Objects.requireNonNull(SELECTORS.get(name));
        JMSAdmin admin = factory.getAdmin();
        admin.createSubscription(topic, "test_"+name, true, selector, true);
        System.out.println("Created subscription: " + name + " with selector " + selector);
    }

    private static void consume(PulsarConnectionFactory factory, Topic topic, String name, int numMessages) throws Exception
    {
        String selector = Objects.requireNonNull(SELECTORS.get(name));
        try (JMSContext context = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
             JMSConsumer consumer = context.createSharedDurableConsumer(topic, name);)
        {
            String selectorInClient = consumer.getMessageSelector();
            System.out.println("Consuming " + numMessages + " messages from subscription: " + name + " with selector " + selectorInClient);
            for (int i = 0; i < numMessages; i++)
            {
                Message message = consumer.receive(2000);
                if (message == null)
                {
                    System.out.println("Didn't receive message from subscription: " + name + " with selector " + selector);
                    return;
                }
                // System.out.println("Received message: " + message);
                message.acknowledge();
            }
        }
    }

    private static void dumpStats(PulsarConnectionFactory factory, Topic topic) throws Exception
    {
        TopicStats internalStats = factory.getPulsarAdmin().topics().getStats(topic.getTopicName());
        internalStats.getSubscriptions().forEach((s, stats) -> {
            String rawSubscriptionName = s.substring(s.lastIndexOf('_') + 1);
            System.out.println("Subscription: " + s + " selector " + SELECTORS.get(rawSubscriptionName));
            long processed = stats.getFilterProcessedMsgCount();
            long accepted = stats.getFilterAcceptedMsgCount();
            long backLog = stats.getMsgBacklog();
            double acceptedRate = processed == 0 ? 1 : (double) accepted / processed;
            long estimatedBackLogWithFilter = (long) (backLog * acceptedRate);
            System.out.println("Backlog: " + backLog);
            System.out.println("Rejected: "+ stats.getFilterRejectedMsgCount());
            System.out.println("Backlog with filter: " + estimatedBackLogWithFilter+" ("+ (int) (acceptedRate * 100) +" %)");
        });
    }
}