package io.kirill.kafkaclient.configs;

import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TwitterConfig {

  public static Authentication auth() {
    var apiToken = System.getenv("TWITTER_API_TOKEN");
    var apiSecret = System.getenv("TWITTER_API_SECRET");
    var accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
    var accessSecret = System.getenv("TWITTER_ACCESS_SECRET");
    return new OAuth1(apiToken, apiSecret, accessToken, accessSecret);
  }
}
