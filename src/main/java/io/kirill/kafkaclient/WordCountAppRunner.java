package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageConsumer;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.kafka.KafkaMessageStreamer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class WordCountAppRunner {

  public static void main(String[] args) {

    var inputTopic = "topic.input.v1";
    var outputTopic = "topic.output.v1";

    var kafkaProducer = KafkaMessageProducer.to(inputTopic, KafkaConfig.highThroughputProducerProps());
    var kafkaConsumer = new KafkaMessageConsumer(KafkaConfig.defaultConsumerProps(), outputTopic);

    var kafkaStreamer = KafkaMessageStreamer
        .<String, String>from(inputTopic)
        .transform(input -> input
          .mapValues((ValueMapper<String, String>) String::toLowerCase)
          .flatMapValues(text -> List.of(text.split("\\W+")))
          .groupBy((key, value) -> value)
          .count()
          .mapValues(Object::toString)
          .toStream())
        .to(outputTopic, Serdes.String(), Serdes.String())
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
}
