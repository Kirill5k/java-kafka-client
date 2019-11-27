package io.kirill.kafkaclient.serdes;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonSerdes {

  public static <T> Serde<T> jsonObject(Class<T> type) {
    var ser = new JsonSerializer<T>();
    var des = new JsonDeserializer<T>(type);
    return Serdes.serdeFrom(ser, des);
  }
}
