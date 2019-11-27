package io.kirill.kafkaclient.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
  private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules().configure(WRITE_DATES_AS_TIMESTAMPS, false);

  private final Class<T> type;

  public JsonDeserializer(Class<T> type) {
    this.type = type;
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    try {
      return objectMapper.readValue(data, type);
    } catch (Exception error) {
      log.error("error deserializing json {} to {}: {}", new String(data), type.getName(), error.getMessage());
      throw new SerializationException("error deserializing json to " + type.getName());
    }
  }
}
