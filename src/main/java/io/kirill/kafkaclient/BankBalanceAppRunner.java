package io.kirill.kafkaclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.kirill.kafkaclient.configs.KafkaConfig;
import io.kirill.kafkaclient.kafka.KafkaMessageProducer;
import io.kirill.kafkaclient.models.Transaction;
import io.kirill.kafkaclient.models.TransactionType;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static java.math.RoundingMode.CEILING;

@Slf4j
public class BankBalanceAppRunner {
  private static final ObjectMapper objectMapper = new ObjectMapper()
      .findAndRegisterModules()
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  private static final Random rand = new Random();
  private static final List<String> users = List.of("alice", "bob", "charlie", "donald");

  public static void main(String[] args) {

    var inputTopic = "user.transactions.v1";
    var outputTopic = "user.balance.v1";

    var kafkaProducer = new KafkaMessageProducer(KafkaConfig.highThroughputProducerProps(), inputTopic);
    kafkaProducer.sendContinuously(BankBalanceAppRunner::randomTransactionAsJson, 250);
  }

  @SneakyThrows
  private static String randomTransactionAsJson() {
    return objectMapper.writeValueAsString(randomTransaction());
  }

  private static Transaction randomTransaction() {
    var user = users.get(rand.nextInt(4));
    var type = rand.nextBoolean() ? TransactionType.WITHDRAW : TransactionType.DEPOSIT;
    var amount = BigDecimal.valueOf((rand.nextInt(100000) + 100)/100).divide(BigDecimal.valueOf(100), CEILING);
    return new Transaction(user, type, amount, Instant.now());
  }
}
