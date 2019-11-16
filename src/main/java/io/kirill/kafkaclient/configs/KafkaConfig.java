package io.kirill.kafkaclient.configs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaConfig {
  public static final String MY_TOPIC = "test-topic.v1";
  public static final String MY_STREAM = "test-stream.v1";
  public static final String GROUP_ID = "tweets";

  private static final String SERVER = "127.0.0.1:9092";

  public static Properties defaultProducerProps() {
    var props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return props;
  }

  public static Properties safeProducerProps() {
    var props = defaultProducerProps();
    props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    props.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    return props;
  }

  public static Properties highThroughputProducerProps() {
    var props = safeProducerProps();
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32*1024));
    return props;
  }

  public static Properties defaultConsumerProps() {
    var props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  public static Properties defaultStreamProps() {
    var props = new Properties();
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, MY_STREAM);
    props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    return props;
  }
}
