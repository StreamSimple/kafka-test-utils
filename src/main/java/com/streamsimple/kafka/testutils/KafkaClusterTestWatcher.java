package com.streamsimple.kafka.testutils;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.runner.Description;
import com.simplifi.it.javautil.net.Port;
import com.simplifi.it.javautils.testutils.DirTestWatcher;
import java.io.File;
import java.io.IOException;

public class KafkaClusterTestWatcher extends DirTestWatcher
{
  public static final String ZOOKEEPER_SNAP_DIR = "zookeeperSnapDir";
  public static final String ZOOKEEPER_LOG_DIR = "zookeeperLogDir";
  public static final String KAFKA_LOG_DIR = "kafkaLogDir";

  private final int numBrokers;
  private final Port kafkaStartPort;

  private final LocalZookeeperCluster.Builder zookeeperClusterBuilder;
  private final LocalKafkaCluster.Builder kafkaClusterBuilder;
  private LocalZookeeperCluster localZookeeperCluster;
  private LocalKafkaCluster localKafkaCluster;

  public KafkaClusterTestWatcher(final int numBrokers,
      final Port kafkaStartPort,
      final Port zookeeperStartPort,
      final int tickTime,
      final int minSessionTimeout,
      final int maxSessionTimeout,
      final ZooKeeperServer.DataTreeBuilder treeBuilder,
      final int numConnections)
  {
    this.numBrokers = numBrokers;
    this.kafkaStartPort = kafkaStartPort;

    zookeeperClusterBuilder = new LocalZookeeperCluster.Builder()
        .setMinSessionTimeout(minSessionTimeout)
        .setMaxSessionTimeout(maxSessionTimeout)
        .setNumConnections(numConnections)
        .setTickTime(tickTime)
        .setTreeBuilder(treeBuilder)
        .startPort(zookeeperStartPort);

    kafkaClusterBuilder = new LocalKafkaCluster.Builder()
        .setNumBrokers(numBrokers)
        .setStartPort(kafkaStartPort);
  }

  @Override
  protected void starting(Description description)
  {
    super.starting(description);

    final File zookeeperSnapDir = this.makeSubDir(ZOOKEEPER_SNAP_DIR);
    final File zookeeperLogDir = this.makeSubDir(ZOOKEEPER_LOG_DIR);
    final File kafkaLogDir = this.makeSubDir(KAFKA_LOG_DIR);

    try {
      localZookeeperCluster = zookeeperClusterBuilder.build(zookeeperSnapDir, zookeeperLogDir, 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    localKafkaCluster = kafkaClusterBuilder.build(kafkaLogDir, localZookeeperCluster, 0);
  }

  @Override
  protected void finished(Description description)
  {
    localKafkaCluster.close();
    localZookeeperCluster.close();

    super.finished(description);
  }

  public static class Builder
  {
    private int numBrokers = 1;
    private Port kafkaStartPort = new Port(9093);
    private int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    private int minSessionTimeout = -1;
    private int maxSessionTimeout = -1;
    private ZooKeeperServer.DataTreeBuilder treeBuilder = new ZooKeeperServer.BasicDataTreeBuilder();
    private Port zookeeperStartPort = new Port(2181);
    private int numConnections = 100;

    public Builder setNumBrokers(int numBrokers)
    {
      this.numBrokers = numBrokers;
      return this;
    }

    public Builder setKafkaStartPort(Port kafkaStartPort)
    {
      this.kafkaStartPort = kafkaStartPort;
      return this;
    }

    public Builder setZookeeperStartPort(final Port zookeeperStartPort)
    {
      this.zookeeperStartPort = zookeeperStartPort;
      return this;
    }

    public Builder setTickTime(int tickTime)
    {
      this.tickTime = tickTime;
      return this;
    }

    public Builder setMinSessionTimeout(int minSessionTimeout)
    {
      this.minSessionTimeout = minSessionTimeout;
      return this;
    }

    public Builder setMaxSessionTimeout(int maxSessionTimeout)
    {
      this.maxSessionTimeout = maxSessionTimeout;
      return this;
    }

    public Builder setTreeBuilder(ZooKeeperServer.DataTreeBuilder treeBuilder)
    {
      this.treeBuilder = treeBuilder;
      return this;
    }

    public Builder setNumConnections(int numConnections)
    {
      this.numConnections = numConnections;
      return this;
    }

    public KafkaClusterTestWatcher build()
    {
      return new KafkaClusterTestWatcher(numBrokers,
          kafkaStartPort,
          zookeeperStartPort,
          tickTime,
          minSessionTimeout,
          maxSessionTimeout,
          treeBuilder,
          numConnections);
    }
  }
}
