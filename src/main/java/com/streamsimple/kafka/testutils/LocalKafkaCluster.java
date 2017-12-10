/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsimple.kafka.testutils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.streamsimple.commons.lang3.StringUtils;
import com.streamsimple.commons.lang3.reflect.FieldUtils;
import com.streamsimple.guava.common.base.Preconditions;
import com.streamsimple.guava.common.collect.Lists;
import com.streamsimple.javautil.net.Endpoint;
import com.streamsimple.javautil.net.Host;
import com.streamsimple.javautil.net.Port;
import com.streamsimple.javautil.net.hunt.NaivePortHunter;
import com.streamsimple.javautil.poll.Poller;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;

public class LocalKafkaCluster
{
  public static final byte RUNNING_AS_BROKER = 3;
  public static final byte RUNNING_AS_CONTROLLER = 4;

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
      final int clusterId, final Port startPort)
  {
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

      // Wait for the server to actually start
      final KafkaServer innerServer;

      try {
        innerServer = (KafkaServer)FieldUtils.readField(kafkaServer, "server", true);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }

      try {
        new Poller<Void>()
            .setInterval(100L)
            .setTimeout(30000L)
            .poll(new Poller.Func<Void>()
            {
              public Poller.Result<Void> run()
              {
                byte state = innerServer.brokerState().currentState();

                if (state == RUNNING_AS_BROKER || state == RUNNING_AS_CONTROLLER) {
                  return Poller.Result.done();
                } else {
                  return Poller.Result.notDone();
                }
              }
            });
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }

      brokers.add(kafkaServer);
    }

    running = true;
  }

  private KafkaServerStartable createBroker(final int brokerId, final Port port)
  {
    File brokerLogDir = new File(clusterLogDir, "broker-" + brokerId);
    brokerLogDir.mkdirs();

    Properties props = new Properties();
    props.setProperty("broker.id", Integer.toString((clusterId * 10000) + brokerId));
    props.setProperty("log.dirs", brokerLogDir.getAbsolutePath());
    props.setProperty("zookeeper.connect", "localhost:" + zookeeperCluster.getPort().toInt());
    props.setProperty("port", port.toString());
    props.setProperty("default.replication.factor", "1");
    props.setProperty("log.flush.interval.messages", "50000");
    props.setProperty("offsets.topic.num.partitions", "1");
    props.setProperty("offsets.topic.replication.factor", "1");

    return new KafkaServerStartable(new KafkaConfig(props));
  }

  public List<Endpoint> getBootstrapEndpoints()
  {
    List<Endpoint> endpoints = Lists.newArrayList();

    for (Port port: brokerPorts) {
      endpoints.add(new Endpoint(Host.LOCAL, port));
    }

    return endpoints;
  }

  public String getBootstrapEndpointsProp()
  {
    return StringUtils.join(getBootstrapEndpoints(), ',');
  }

  public void createTopic(final String topicName, final int partitionCount)
  {
    validateIsRunning();
    String[] args = new String[9];
    args[0] = "--zookeeper";
    args[1] = "localhost:" + zookeeperCluster.getPort();
    args[2] = "--replication-factor";
    args[3] = "1";
    args[4] = "--partitions";
    args[5] = Integer.toString(partitionCount);
    args[6] = "--topic";
    args[7] = topicName;
    args[8] = "--create";
    ZkUtils zu = ZkUtils.apply("localhost:" + zookeeperCluster.getPort(), 30000, 30000, false);
    TopicCommand.createTopic(zu, new TopicCommand.TopicCommandOptions(args));
  }

  private void validateIsRunning()
  {
    Preconditions.checkState(running, "Cluster is not running.");
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
