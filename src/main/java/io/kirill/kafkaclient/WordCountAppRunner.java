package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageConsumer;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Stream;

@Slf4j
public class WordCountAppRunner {

  public static void main(String[] args) {

    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.highThroughputProducerProps(), KafkaConfig.INPUT_TOPIC);
    var kafkaConsumer = new KafkaMessageConsumer(KafkaConfig.defaultConsumerProps(), KafkaConfig.OUTPUT_TOPIC);

    var streamsBuilder = new StreamsBuilder();
    var input = streamsBuilder.<String, String>stream(KafkaConfig.INPUT_TOPIC);

    KStream<String, String> wordCounts = input
        .mapValues(text -> text.toLowerCase())
        .flatMapValues(text -> Arrays.asList(text.split("\\W+")))
        .groupBy((key, value) -> value)
        .count()
        .mapValues(count -> count.toString())
        .toStream();

    wordCounts.to(KafkaConfig.OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    var stream = new KafkaStreams(streamsBuilder.build(), KafkaConfig.defaultStreamProps());
    stream.start();

    kafkaConsumer.onMessage((key, value) -> log.info("received msg: key - {}; value - {}", key, value));

    Stream.of(
        "hello world", "testing kafka streams", "hope it is working"
    ).forEach(kafkaProducer::send);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaProducer.stop();
      kafkaConsumer.stop();
      stream.close();
    }));
  }
}
