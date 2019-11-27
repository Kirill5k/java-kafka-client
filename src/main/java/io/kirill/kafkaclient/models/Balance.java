package io.kirill.kafkaclient.models;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class Balance {
  private final String userName;
  private final Long transactionsCount;
  private final BigDecimal balance;
  private final Instant lastUpdateTime;
}
