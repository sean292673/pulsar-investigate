package com.example.demo;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsumerService {

    private static final String TOPIC = "istop1";

    private final Logger l = LoggerFactory.getLogger(ConsumerService.class);
    private final PulsarClient client;

    @PostConstruct
    public void postC() throws Exception {
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(TOPIC)
                .enableBatching(false)
                .create();
        producer.newMessage().key("key-1").value("message-1-1").send();
        producer.newMessage().key("key-1").value("message-1-2").send();
        producer.newMessage().key("key-2").value("message-2-1").send();
        producer.newMessage().key("key-2").value("message-2-2").send();
        producer.newMessage().key("key-3").value("message-3-1").send();
        producer.newMessage().key("key-3").value("message-3-2").send();
        producer.newMessage().key("key-4").value("message-4-1").send();
        producer.newMessage().key("key-4").value("message-4-2").send();
        producer.newMessage().key("key-5").value("message-5-1").send();
        producer.newMessage().key("key-5").value("message-5-2").send();
        producer.newMessage().key("key-6").value("message-6-1").send();
        producer.newMessage().key("key-6").value("message-6-2").send();
        producer.newMessage().key("key-7").value("message-7-1").send();
        producer.newMessage().key("key-7").value("message-7-2").send();
        producer.newMessage().key("key-8").value("message-8-1").send();
        producer.newMessage().key("key-8").value("message-8-2").send();
        Thread.sleep(1000);


        int delimeter = 23000;
        Consumer<byte[]> c1 = buildConsumer(0, delimeter);
        Consumer<byte[]> c2 = buildConsumer(delimeter + 1, 65535);
        Thread.sleep(1000);

        new Thread(getConsumerThread(c1, true)).start();
        new Thread(getConsumerThread(c2, false)).start();
    }

    private Runnable getConsumerThread(Consumer<byte[]> consumer, boolean isFast) {
        return () -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    Thread.sleep(1000);
                    Message<byte[]> msgByte;
                    l.info("Trying to receive by consumer:{} in thread:{}", consumer.getConsumerName(), Thread.currentThread().getId());
                    msgByte = consumer.receive();
                    if (i == 1 && isFast) {
                        l.info("Hanging consumer:{} in thread:{}", consumer.getConsumerName(), Thread.currentThread().getId());
                        Thread.sleep(10000);
                    }
                    consumer.acknowledge(msgByte);
                    String msgStr = new String(msgByte.getData());
                    l.info("Consumer {} received message:{} with key:{} in thread:{}", consumer.getConsumerName(), msgStr, msgByte.getKey(), Thread.currentThread().getId());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Consumer<byte[]> buildConsumer(int rangeStart, int rangeEnd) throws PulsarClientException {
        return client.newConsumer()
                .topic(TOPIC)
                .subscriptionName("issub11")
                .subscriptionType(SubscriptionType.Key_Shared)
//                .keySharedPolicy(KeySharedPolicy.stickyHashRange().ranges(new Range(rangeStart, rangeEnd)))
                .subscribe();
    }

}
