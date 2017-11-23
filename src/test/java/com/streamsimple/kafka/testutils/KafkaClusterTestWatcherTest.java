package com.streamsimple.kafka.testutils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
    final String testKey = "testKey";
    final String testValue = "testValue";
    final String topicName = "testTopic";
    Thread.sleep(2000L);
    kafkaTestWatcher.createTopic(topicName, 1);

    Properties prodProps = new Properties();
    prodProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    prodProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    prodProps.setProperty("bootstrap.servers", kafkaTestWatcher.getBootstrapServersConfig());
    prodProps.setProperty("linger.ms", "100");

    final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prodProps);
    final Future<RecordMetadata> prodFuture;

    try {
      prodFuture = producer.send(new ProducerRecord<String, String>(testKey, testValue));
      producer.flush();
    } finally {
      //producer.close();
    }

    final RecordMetadata metadata = prodFuture.get();
    System.out.println("TOPIC----- " + metadata.topic());
    System.out.println("OFFSET---- " + metadata.offset());
    System.out.println("FFFF------ " + metadata.partition());

    Properties subProps = new Properties();
    subProps.setProperty("bootstrap.servers", kafkaTestWatcher.getBootstrapServersConfig());
    subProps.setProperty("group.id", "test-consumer");
    subProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    subProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    //subProps.setProperty("auto.offset.reset", "earliest");
    subProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(subProps);

    List partitionInfos = consumer.partitionsFor(topicName);
    HashMap topicPartitions = new HashMap();
    Iterator attemptToPoll = partitionInfos.iterator();

    while(attemptToPoll.hasNext()) {
      PartitionInfo consumerRecords = (PartitionInfo)attemptToPoll.next();
      topicPartitions.put(Integer.valueOf(consumerRecords.partition()), new TopicPartition(topicName, consumerRecords.partition()));
    }

    consumer.assign(new ArrayList(topicPartitions.values()));
    attemptToPoll = topicPartitions.values().iterator();

    while(attemptToPoll.hasNext()) {
      TopicPartition var10 = (TopicPartition)attemptToPoll.next();
      consumer.seekToBeginning(new TopicPartition[]{var10});
    }

    final ConsumerRecords<String, String> records;

    try {
      records = consumer.poll(TIMEOUT);
    } finally {
      consumer.close();
    }

    Assert.assertEquals(1, records.count());

    Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
    ConsumerRecord<String, String> record = recordIterator.next();

    Assert.assertEquals(testKey, record.key());
    Assert.assertEquals(testValue, record.value());
  }
}
