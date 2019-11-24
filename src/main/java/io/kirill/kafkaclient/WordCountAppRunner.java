package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageConsumer;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.kafka.KafkaMessageStreamer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
public class WordCountAppRunner {

  public static void main(String[] args) {

    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.highThroughputProducerProps(), KafkaConfig.INPUT_TOPIC);
    var kafkaConsumer = new KafkaMessageConsumer(KafkaConfig.defaultConsumerProps(), KafkaConfig.OUTPUT_TOPIC);

    var kafkaStreamer = KafkaMessageStreamer
        .<String, String>from(KafkaConfig.INPUT_TOPIC)
        .transform(input -> input
          .mapValues((ValueMapper<String, String>) String::toLowerCase)
          .flatMapValues(text -> Arrays.asList(text.split("\\W+")))
          .groupBy((key, value) -> value)
          .count()
          .mapValues(Object::toString)
          .toStream())
        .to(KafkaConfig.OUTPUT_TOPIC, Serdes.String(), Serdes.String())
        .start(KafkaConfig.defaultStreamProps());

    kafkaConsumer.onMessage((key, value) -> log.info("received msg: key - {}; value - {}", key, value));

    Stream.of(
        "hello world", "testing kafka streams", "hope it is working"
    ).forEach(kafkaProducer::send);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaProducer.stop();
      kafkaConsumer.stop();
      kafkaStreamer.stop();
    }));
  }

  private Function<KStream<String, String>, KStream<String, String>> wordCounter = input -> input
      .mapValues((ValueMapper<String, String>) String::toLowerCase)
      .flatMapValues(text -> Arrays.asList(text.split("\\W+")))
      .groupBy((key, value) -> value)
      .count()
      .mapValues(Object::toString)
      .toStream();

}
