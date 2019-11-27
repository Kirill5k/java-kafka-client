package io.kirill.kafkaclient;

import static java.math.RoundingMode.CEILING;

import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.kafka.KafkaMessageStreamer;
import io.kirill.kafkaclient.models.Transaction;
import io.kirill.kafkaclient.models.TransactionType;
import io.kirill.kafkaclient.serdes.JsonSerdes;
import io.kirill.kafkaclient.serdes.JsonSerializer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;

@Slf4j
public class BankBalanceAppRunner {
  private static final Random rand = new Random();
  private static final List<String> users = List.of("alice", "bob", "charlie", "donald");

  public static void main(String[] args) {

    var inputTopic = "user.transactions.v1";
    var outputTopicV1 = "user.balance.v1";
    var outputTopicV2 = "user.balance.v2";
    var kafkaProducer = KafkaMessageProducer.<String, Transaction>to(inputTopic, KafkaConfig.highThroughputProducerProps(), new StringSerializer(), new JsonSerializer<>());
    kafkaProducer.sendContinuously(BankBalanceAppRunner::randomTransactionMessage, 250);

    // start with the initial balance and then aggregate
    /*
    var kafkaStreamer = KafkaMessageStreamer
        .<String, Transaction>from(inputTopic, Serdes.String(), JsonSerdes.jsonObject(Transaction.class))
        .transform(input -> input
            .groupByKey()
            .aggregate(
                () -> Balance.INITIAL,
                (key, transaction, balance) -> balance.addTransaction(transaction)
            )
            .toStream())
        .to(outputTopicV2, Serdes.String(), JsonSerdes.jsonObject(Balance.class))
        .start(KafkaConfig.defaultStreamProps());

     */

    var kafkaStreamer = KafkaMessageStreamer
        .<String, Transaction>from(inputTopic, Serdes.String(), JsonSerdes.jsonObject(Transaction.class))
        .to(outputTopicV1, Serdes.String(), JsonSerdes.jsonObject(Transaction.class))
        .start(KafkaConfig.defaultStreamProps());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaStreamer.stop();
      kafkaProducer.stop();
    }));
  }

  @SneakyThrows
  private static KeyValue<String, Transaction> randomTransactionMessage() {
    var transaction = randomTransaction();
    return KeyValue.pair(transaction.getUserName(), transaction);
  }

  private static Transaction randomTransaction() {
    var user = users.get(rand.nextInt(4));
    var type = rand.nextBoolean() ? TransactionType.WITHDRAW : TransactionType.DEPOSIT;
    var amount = BigDecimal.valueOf((rand.nextInt(100000) + 100)/100).divide(BigDecimal.valueOf(100), CEILING);
    return new Transaction(user, type, amount, Instant.now());
  }
}
