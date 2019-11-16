package io.kirill.kafkaclient.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;

@Slf4j
public class TwitterConsumer {
  private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
  private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
  private final BasicClient twitterClient;

  public TwitterConsumer(Authentication auth, String... terms) {
    var hosts = new HttpHosts(Constants.STREAM_HOST);
    var endpoint = new StatusesFilterEndpoint();
    endpoint.trackTerms(List.of(terms));

    twitterClient = new ClientBuilder()
        .name("java-kafka-consumer")
        .hosts(hosts)
        .authentication(auth)
        .endpoint(endpoint)
        .processor(new StringDelimitedProcessor(msgQueue))
        .eventMessageQueue(eventQueue)
        .build();
  }

  @SneakyThrows
  public void onMessage(Consumer<String> messageConsumer) {
    log.info("starting twitter client");
    twitterClient.connect();
    while (!twitterClient.isDone()) {
      ofNullable(msgQueue.take()).ifPresent(msg -> {
        log.info("received message [{}]", msg);
        messageConsumer.accept(msg);
      });
    }
  }

  public void stop() {
    log.info("stopping twitter consumer");
    twitterClient.stop();;
  }
}
