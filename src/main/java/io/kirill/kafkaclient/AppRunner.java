package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.configs.TwitterConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.twitter.TwitterConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.kirill.kafkaclient.configs.KafkaConfig.MY_TOPIC;

@Slf4j
public class AppRunner {

  @SneakyThrows
  public static void main(String[] args) {
    var twitterConsumer = new TwitterConsumer(TwitterConfig.auth(), "bitcoin");
    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.defaultProducerProps(), MY_TOPIC);

    twitterConsumer.run(kafkaProducer::send);

    Thread.sleep(5000);
    twitterConsumer.stop();
    kafkaProducer.stop();
  }
}
