package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.kirill.kafkaclient.configs.KafkaConfig.MY_TOPIC;
import static io.kirill.kafkaclient.configs.KafkaConfig.defaultConsumerProps;

@Slf4j
public class KafkaConsumerClient {

  public static void main(String[] args) throws Exception {
    var latch = new CountDownLatch(1);
    var consumer = new ConsumerThread(latch, defaultConsumerProps(), MY_TOPIC);

    var thread = new Thread(consumer);
    thread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutDown));

    latch.await();
  }

  @Slf4j
  public static class ConsumerThread implements Runnable {
    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(CountDownLatch latch, Properties consumerConfig, String topic) {
      this.latch = latch;
      consumer = new KafkaConsumer<>(consumerConfig);
      consumer.subscribe(List.of(topic));
    }

    @Override
    public void run() {
      try {
        while (true) {
          var records = consumer.poll(Duration.ofMillis(100));
          records.forEach(rec -> log.info("received message from part {} with key {} and offset {}: {}", rec.partition(), rec.key(), rec.offset(), rec.value()));
        }
      } catch (WakeupException exception) {
        log.error("received shutdown signal");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutDown() {
      consumer.wakeup();
    }
  }
}
