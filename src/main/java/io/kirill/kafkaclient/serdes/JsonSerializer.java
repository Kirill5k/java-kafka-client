package io.kirill.kafkaclient.serdes;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
@NoArgsConstructor
public class JsonSerializer<T> implements Serializer<T> {
  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules().configure(WRITE_DATES_AS_TIMESTAMPS, false);


  @Override
  public byte[] serialize(String topic, T data) {
    try {
      return objectMapper.writeValueAsBytes(data);
    } catch (Exception error) {
      log.error("error serializing to json {}", error.getMessage());
      throw new SerializationException("error serializing to json: " + error.getMessage());
    }
  }
}
