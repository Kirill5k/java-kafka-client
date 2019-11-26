package io.kirill.kafkaclient.models;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class Transaction {
  private final String userName;
  private final TransactionType type;
  private final BigDecimal amount;
  private final Instant time;
}
