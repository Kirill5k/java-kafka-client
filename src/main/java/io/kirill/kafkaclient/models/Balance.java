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

  public Balance addTransaction(Transaction transaction) {
    if (userName != null && !userName.equals(transaction.getUserName())) {
      throw new IllegalArgumentException();
    }

    var newBalance = transaction.getType() == WITHDRAW ? balance.subtract(transaction.getAmount()) : balance.add(transaction.getAmount());
    return new Balance(transaction.getUserName(), transactionsCount+1, newBalance, transaction.getTime());
  }
}
