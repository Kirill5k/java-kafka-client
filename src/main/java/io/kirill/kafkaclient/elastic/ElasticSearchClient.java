package io.kirill.kafkaclient.elastic;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class ElasticSearchClient {
  private final RestHighLevelClient client;

  public ElasticSearchClient(String host, CredentialsProvider credentialsProvider) {
    var clientBuilder = RestClient.builder(new HttpHost(host, 443, "https"))
        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

    client = new RestHighLevelClient(clientBuilder);
  }

  @SneakyThrows
  public void send(String index, String msg) {
    try {
      log.info("sending msg {} to index {}", msg, index);
      var req = new IndexRequest(index).source(msg, XContentType.JSON);
      var res = client.index(req, RequestOptions.DEFAULT);
      log.info("success response from elastic search {}/{}", index, res.getId());
    } catch (Exception error) {
      log.error("error sending message {}: {}", msg, error.getMessage());
    }
  }

  @SneakyThrows
  public void stop() {
    log.info("stopping elastic search client");
    client.close();
  }
}
