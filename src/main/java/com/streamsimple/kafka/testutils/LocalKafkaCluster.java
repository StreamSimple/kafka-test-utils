package com.streamsimple.kafka.testutils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.simplifi.it.javautil.net.Port;
import com.simplifi.it.javautil.net.hunt.NaivePortHunter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class LocalKafkaCluster
{
  private final int numBrokers;
  private final File logDirs;
  private final LocalZookeeperCluster zookeeperCluster;
  private final int clusterId;
  private final List<KafkaServerStartable> brokers = Lists.newArrayList();
  private final Port startPort;

  private File clusterLogDir;
  private List<Port> brokerPorts;
  private boolean running = false;

  public LocalKafkaCluster(final int numBrokers, final File logDirs,
                           final LocalZookeeperCluster zookeeperCluster,
                           final int clusterId, final Port startPort) {
    this.numBrokers = numBrokers;
    this.logDirs = logDirs;
    this.zookeeperCluster = zookeeperCluster;
    this.clusterId = clusterId;
    this.startPort = startPort;
  }

  public void setup() throws IOException
  {
    zookeeperCluster.setup();

    clusterLogDir = new File(logDirs, "cluster-" + clusterId);
    clusterLogDir.mkdirs();

    final NaivePortHunter portHunter = new NaivePortHunter();
    brokerPorts = portHunter.getPorts(startPort, numBrokers);

    for (int brokerCount = 0; brokerCount < numBrokers; brokerCount++) {
      final Port brokerPort = brokerPorts.get(brokerCount);
      final KafkaServerStartable kafkaServer = createBroker(brokerCount, brokerPort);
      kafkaServer.startup();
      brokers.add(kafkaServer);
    }

    running = true;
  }

  private KafkaServerStartable createBroker(final int brokerId, final Port port)
  {
    File brokerLogDir = new File(clusterLogDir, "broker-" + brokerId);
    brokerLogDir.mkdirs();

    Properties props = new Properties();
    props.setProperty("broker.id", clusterId + "-" + brokerId);
    props.setProperty("log.dirs", brokerLogDir.getAbsolutePath());
    props.setProperty("zookeeper.connect", "localhost:" + zookeeperCluster.getPort().toInt());
    props.setProperty("port", port.toString());
    props.setProperty("default.replication.factor", "1");
    props.setProperty("log.flush.interval.messages", "50000");

    return new KafkaServerStartable(new KafkaConfig(props));
  }

  public List<Port> getBrokerPorts()
  {
    Preconditions.checkState(running, "Cluster is not running.");
    return Lists.newArrayList(brokerPorts);
  }

  public void close()
  {
    running = false;

    for (KafkaServerStartable broker: brokers) {
      broker.shutdown();
      broker.awaitShutdown();
    }

    zookeeperCluster.close();
  }

  public static class Builder
  {
    private int numBrokers = 1;
    private Port startPort = new Port(9093);

    public Builder setNumBrokers(int numBrokers)
    {
      this.numBrokers = numBrokers;
      return this;
    }

    public Builder setStartPort(Port startPort)
    {
      this.startPort = Preconditions.checkNotNull(startPort);
      return this;
    }

    public LocalKafkaCluster build(final File logDirs, final LocalZookeeperCluster zookeeperCluster,
                                   final int clusterId)
    {
      return new LocalKafkaCluster(numBrokers, logDirs, zookeeperCluster, clusterId, startPort);
    }
  }
}
