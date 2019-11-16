package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.ElasticConfig;
import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.configs.TwitterConfig;
import io.kirill.kafkaclient.elastic.ElasticSearchClient;
import io.kirill.kafkaclient.kafka.KafkaMessageConsumer;
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
    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.highThroughputProducerProps(), MY_TOPIC);
    var kafkaConsumer = new KafkaMessageConsumer(KafkaConfig.defaultConsumerProps(), MY_TOPIC);
    var elasticClient = new ElasticSearchClient(ElasticConfig.HOST, ElasticConfig.credentials());

    kafkaConsumer.onMessage(msg -> elasticClient.send("twitter", msg));
    twitterConsumer.onMessage(kafkaProducer::send);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      twitterConsumer.stop();
      kafkaProducer.stop();
      kafkaConsumer.stop();
      elasticClient.stop();
    }));
  }
}
