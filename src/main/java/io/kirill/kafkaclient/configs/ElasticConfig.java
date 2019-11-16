package io.kirill.kafkaclient.configs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ElasticConfig {
  public static final String HOST = System.getenv("ELASTIC_HOST");

  public static CredentialsProvider credentials() {
    var username = System.getenv("ELASTIC_USER");
    var password = System.getenv("ELASTIC_PASS");
    var credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    return credentialsProvider;
  }
}
