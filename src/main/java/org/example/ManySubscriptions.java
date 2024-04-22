package org.example;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import com.datastax.oss.pulsar.jms.api.JMSAdmin;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.client.admin.PulsarAdminException;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.BacklogQuota;
import com.datastax.oss.pulsar.jms.shaded.org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableMap;

public class ManySubscriptions
{

    private static final Map<String, String> SELECTORS = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception
    {
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

            for (int i = 0; i < 10; i++) {
                createSubscription(factory, topic1, "sub1x" + i, "(dummy = 'impossible') or (dummy2 = 'impossible2') or (dummy3 = 'impossible') or (param < 1)");
                createSubscription(factory, topic1, "sub10x" + i, "(dummy = 'impossible') or (dummy2 = 'impossible2') or (dummy3 = 'impossible') or (param < 10)");
                createSubscription(factory, topic1, "sub50x" + i, "(dummy = 'impossible') or (dummy2 = 'impossible2') or (dummy3 = 'impossible') or (param < 50)");
                createSubscription(factory, topic1, "sub100x" + i, "(dummy = 'impossible') or (dummy2 = 'impossible2') or (dummy3 = 'impossible') or (param < 100)");
            }

            for (String subscription : SELECTORS.keySet())
            {
                consume(factory, topic1, subscription, 0);
            }

            dumpStats(factory, topic1);


            int numberMessages = 1_000_000;
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
                    producer.setProperty("param", param);
                    // make the message metadata bigger and exercise the filter with conditions that are not eventually met
                    for (int k = 0 ; k < 5; k++) {
                        producer.setProperty("dummy" + k, "foo" + k);
                    }
                    producer.send(topic1, "text-" + param);
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

            // consume


            for (String subscription : SELECTORS.keySet())
            {
                consume(factory, topic1, subscription, 20);
            }

            // dump again

            dumpStats(factory, topic1);

            System.out.println("***************************************************************************");
            Thread.sleep(5000);
            System.out.println("***************************************************************************");

            dumpStats(factory, topic1);

            for (String subscription : SELECTORS.keySet())
            {
                consume(factory, topic1, subscription, -1);
            }

            dumpStats(factory, topic1);

        }
    }


    private static void createSubscription(PulsarConnectionFactory factory, Topic topic, String name, String selector) throws Exception
    {
        SELECTORS.put(name, selector);
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
            TopicStats internalStats = factory.getPulsarAdmin().topics().getStats(topic.getTopicName());
            long backlog = internalStats.getSubscriptions().get("test_"+name).getMsgBacklog();
            System.out.println("Consuming " + numMessages + " messages from subscription: " + name + " with selector " + selector+" backlog = "+backlog);

            if (numMessages == 0) {
                // receive, not ack
                consumer.receiveNoWait();
                return;
            }

            if (numMessages == -1) {
                long count = 0;
                while (true) {
                    Message message = consumer.receive(2000);
                    if (message == null) {
                        System.out.println("Consumed all messages from subscription: " + name + " with selector " + selector);
                        break;
                    }
                    if (count % 10000 == 0) {
                        System.out.println("Consumed " + count + "/" + backlog + " (" + count *  100 /backlog + "%) messages from subscription: " + name + " with selector " + selector);
                    }
                    message.acknowledge();
                    count++;
                }
                return;
            }

            for (int i = 0; i < numMessages; i++)
            {
                Message message = consumer.receive(2000);
                if (message == null)
                {
                    throw new IllegalStateException("Didn't receive message from subscription: " + name + " with selector " + selector);
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

            if (stats.getFilterRejectedMsgCount() != 0) {
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
                System.out.println("*****************************************************************************************************");
            }
        });
    }
}