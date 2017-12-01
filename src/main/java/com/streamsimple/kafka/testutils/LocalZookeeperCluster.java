package com.streamsimple.kafka.testutils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import com.simplifi.it.javautil.net.Port;
import com.simplifi.it.javautil.poll.Poller;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

public class LocalZookeeperCluster
{
  private final File snapDir;
  private final File logDir;
  private final int tickTime;
  private final int minSessionTimeout;
  private final int maxSessionTimeout;
  private final ZooKeeperServer.DataTreeBuilder treeBuilder;
  private final Port startPort;
  private final int numConnections;
  private final int clusterId;

  private ZooKeeperServer server;
  private NIOServerCnxnFactory connectionFactory;

  protected LocalZookeeperCluster(File snapDir, File logDir, int tickTime,
                                 int minSessionTimeout, int maxSessionTimeout,
                                 ZooKeeperServer.DataTreeBuilder treeBuilder,
                                 Port startPort, int numConnections,
                                 int clusterId) throws IOException
  {
    this.snapDir = snapDir;
    this.logDir = logDir;
    this.tickTime = tickTime;
    this.minSessionTimeout = minSessionTimeout;
    this.maxSessionTimeout = maxSessionTimeout;
    this.treeBuilder = treeBuilder;
    this.startPort = startPort;
    this.numConnections = numConnections;
    this.clusterId = clusterId;
  }

  public void setup() throws IOException
  {
    int currentPort = startPort.toInt();

    final ZooKeeperServer zooKeeperServer = new LocalZookeeperServer(
        snapDir, logDir, tickTime, minSessionTimeout, maxSessionTimeout, treeBuilder);

    final NIOServerCnxnFactory connectionFactory = portHunt(currentPort);

    try {
      connectionFactory.startup(zooKeeperServer);
    } catch (InterruptedException e) {
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
              if (zooKeeperServer.isRunning()) {
                return Poller.Result.done();
              } else {
                return Poller.Result.notDone();
              }
            }
          });
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }

    this.server = zooKeeperServer;
    this.connectionFactory = connectionFactory;
  }

  public Port getPort()
  {
    return new Port(connectionFactory.getLocalPort());
  }

  public void close()
  {
    server.shutdown();
    connectionFactory.closeAll();
    connectionFactory.shutdown();
  }

  private NIOServerCnxnFactory portHunt(int currentPort) {

    for (;;currentPort++) {
      if (currentPort > Port.MAX_PORT) {
        throw new IllegalStateException("No available ports");
      }

      try {
        final NIOServerCnxnFactory connectionFactory = new NIOServerCnxnFactory();
        connectionFactory.configure(new InetSocketAddress(currentPort), numConnections);
        return connectionFactory;
      } catch (IOException e) {
      }
    }
  }

  public static class Builder
  {
    private int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    private int minSessionTimeout = -1;
    private int maxSessionTimeout = -1;
    private ZooKeeperServer.DataTreeBuilder treeBuilder = new ZooKeeperServer.BasicDataTreeBuilder();
    private Port startPort = new Port(2181);
    private int numConnections = 100;

    public Builder startPort(final Port startPort)
    {
      this.startPort = startPort;
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

    public LocalZookeeperCluster build(final File snapDir, final File logDir,
                                       final int clusterId) throws IOException
    {
      return new LocalZookeeperCluster(snapDir, logDir, tickTime, minSessionTimeout,
          maxSessionTimeout, treeBuilder, startPort, numConnections, clusterId);
    }
  }
}
