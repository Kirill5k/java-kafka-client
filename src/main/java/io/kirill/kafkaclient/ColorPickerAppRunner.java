package io.kirill.kafkaclient;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageConsumer;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.kafka.KafkaMessageStreamer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;

@Slf4j
public class ColorPickerAppRunner {

  public static void main(String[] args) {

    var inputTopic = "user.color.v1";
    var outputTopic = "color.count.v1";

    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.highThroughputProducerProps(), inputTopic);
    var kafkaConsumer = new KafkaMessageConsumer(KafkaConfig.defaultConsumerProps(), outputTopic);

    var colorCountsStream = KafkaMessageStreamer
        .<String, String>from(inputTopic)
        .transform(input -> input
          .mapValues(value -> value.split(","))
          .filter((key, value) -> List.of("blue", "red", "green").contains(value[1]))
          .selectKey((key, value) -> value[1])
          .groupByKey()
          .count()
          .toStream())
        .to(outputTopic, Serdes.String(), Serdes.Long())
        .start(KafkaConfig.defaultStreamProps());

    kafkaConsumer.onMessage((key, value) -> log.info("received msg: key - {}; value - {}", key, value));
    kafkaProducer.send("alice,red");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      colorCountsStream.stop();
      kafkaConsumer.stop();
      kafkaProducer.stop();
    }));
  }
}
