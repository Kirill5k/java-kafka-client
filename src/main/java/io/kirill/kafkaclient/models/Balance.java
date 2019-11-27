package io.kirill.kafkaclient.models;

import static io.kirill.kafkaclient.models.TransactionType.WITHDRAW;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor
public class Balance {
  public static final Balance INITIAL = new Balance(null, 0L, BigDecimal.ZERO, Instant.ofEpochMilli(0));

  private final String userName;
  private final Long transactionsCount;
  private final BigDecimal balance;
  private final Instant lastUpdateTime;

  public Balance addTransaction(Transaction tx) {
    if (userName != null && !userName.equals(tx.getUserName())) {
      throw new IllegalArgumentException();
    }

    var newBalance = tx.getType() == WITHDRAW ? balance.subtract(tx.getAmount()) : balance.add(tx.getAmount());
    var newUpdateTime = tx.getTime().isAfter(lastUpdateTime) ? lastUpdateTime : tx.getTime();
    return new Balance(tx.getUserName(), transactionsCount+1, newBalance, newUpdateTime);
  }
}
