package io.kirill.kafkaclient.kafka;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.function.Function;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaMessageStreamer<IK, IV, OK, OV> {
  private final StreamsBuilder streamsBuilder;
  private final KStream<IK, IV> input;
  private final KStream<OK, OV> output;

  private KafkaStreams stream;

  private KafkaMessageStreamer(String inputTopic) {
    streamsBuilder = new StreamsBuilder();
    input = streamsBuilder.<IK, IV>stream(inputTopic);
    output = null;
  }

  private KafkaMessageStreamer(Serde<IK> keySerde, Serde<IV> valueSerde, String inputTopic) {
    streamsBuilder = new StreamsBuilder();
    input = streamsBuilder.<IK, IV>stream(inputTopic, Consumed.with(keySerde, valueSerde));
    output = null;
  }

  public static <K, V> KafkaMessageStreamer<K, V, K, V> from(String inputTopic, Serde<K> keySerde, Serde<V> valueSerde) {
    return new KafkaMessageStreamer<>(keySerde, valueSerde, inputTopic);
  }

  public static <K, V> KafkaMessageStreamer<K, V, K, V> from(String inputTopic) {
    return new KafkaMessageStreamer<>(inputTopic);
  }

  public <OK, OV> KafkaMessageStreamer<IK, IV, OK, OV> transform(Function<KStream<IK, IV>, KStream<OK, OV>> transformer) {
    return new KafkaMessageStreamer<>(streamsBuilder, input, transformer.apply(input));
  }

  public KafkaMessageStreamer<IK, IV, OK, OV> to(String outputTopic, Serde<OK> keySerde, Serde<OV> valueSerde) {
    output.to(outputTopic, Produced.with(keySerde, valueSerde));
    return this;
  }

  public KafkaMessageStreamer<IK, IV, OK, OV> start(Properties props) {
    stream = new KafkaStreams(streamsBuilder.build(), props);
    stream.start();
    return this;
  }

  public void stop() {
    stream.cleanUp();
    stream.close();;
  }
}
