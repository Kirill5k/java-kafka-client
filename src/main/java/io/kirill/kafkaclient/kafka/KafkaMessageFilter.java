package io.kirill.kafkaclient.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;

@Slf4j
public class KafkaMessageFilter {
  private final KStream<String, String> inputTopic;
  private final KafkaStreams stream;

  public KafkaMessageFilter(Properties props, String inputTopic) {
    var streamsBuilder = new StreamsBuilder();
    this.inputTopic = streamsBuilder.stream(inputTopic);
    this.stream = new KafkaStreams(streamsBuilder.build(), props);
  }

  public void filter(String outputTopic, Predicate<String, String> filterPredicate) {
    inputTopic.filter(filterPredicate).to(outputTopic);
    stream.start();
  }

  public void stop() {
    stream.close();
  }
}
