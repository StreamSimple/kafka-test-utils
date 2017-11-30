package com.streamsimple.kafka.testutils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaClusterTestWatcherTest
{
  public static final long TIMEOUT = 30000L;

  @Rule
  public final KafkaClusterTestWatcher kafkaTestWatcher = new KafkaClusterTestWatcher.Builder().build();

  @Test
  public void simpleKafkaSendAndRecieveTest() throws ExecutionException, InterruptedException
  {
    final String testValue = "testValue";
    final String topicName = "testTopic";
    kafkaTestWatcher.createTopic(topicName, 1);

    Properties prodProps = new Properties();
    prodProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
    prodProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestWatcher.getBootstrapEndpointsProp());
    prodProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(100));

    final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prodProps);
    final Future<RecordMetadata> prodFuture;

    try {
      prodFuture = producer.send(new ProducerRecord<String, String>(topicName, testValue));
      producer.flush();
      prodFuture.get();
    } finally {
      producer.close();
    }

    Properties subProps = new Properties();
    subProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestWatcher.getBootstrapEndpointsProp());
    subProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
    subProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    subProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    subProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    subProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(subProps);
    consumer.subscribe(Lists.newArrayList(topicName));

    final ConsumerRecords<String, String> records;

    try {
      records = consumer.poll(TIMEOUT);
    } finally {
      consumer.close();
    }

    Assert.assertEquals(1, records.count());

    Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
    ConsumerRecord<String, String> record = recordIterator.next();

    Assert.assertEquals(testValue, record.value());
  }
}
