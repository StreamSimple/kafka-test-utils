package com.streamsimple.kafka.testutils;

import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import java.io.File;
import java.io.IOException;

public class LocalZookeeperServer extends ZooKeeperServer
{
  protected LocalZookeeperServer(File snapDir, File logDir, int tickTime,
                              int minSessionTimeout, int maxSessionTimeout,
                              DataTreeBuilder treeBuilder) throws IOException
  {
    this(new FileTxnSnapLog(snapDir, logDir), tickTime,
        minSessionTimeout, maxSessionTimeout, treeBuilder);
  }

  protected LocalZookeeperServer(FileTxnSnapLog fileTxnSnapLog, int tickTime,
                                 int minSessionTimeout, int maxSessionTimeout,
                                 DataTreeBuilder treeBuilder) throws IOException
  {
    super(fileTxnSnapLog, tickTime, minSessionTimeout,
        maxSessionTimeout, treeBuilder, new ZKDatabase(fileTxnSnapLog));
  }

  @Override
  protected void registerJMX()
  {
    // Do nothing
  }

  @Override
  protected void unregisterJMX()
  {
    // Do nothing
  }

  public static class Builder
  {
    private int tickTime = ZooKeeperServer.DEFAULT_TICK_TIME;
    private int minSessionTimeout = -1;
    private int maxSessionTimeout = -1;
    private DataTreeBuilder treeBuilder = new BasicDataTreeBuilder();

    public Builder setTickTime(final int tickTime)
    {
      this.tickTime = tickTime;
      return this;
    }

    public Builder setMinSessionTimeout(final int minSessionTimeout)
    {
      this.minSessionTimeout = minSessionTimeout;
      return this;
    }

    public Builder setMaxSessionTimeout(final int maxSessionTimeout)
    {
      this.maxSessionTimeout = maxSessionTimeout;
      return this;
    }

    public Builder setDataTreeBuilder(final DataTreeBuilder treeBuilder)
    {
      this.treeBuilder = treeBuilder;
      return this;
    }

    public ZooKeeperServer build(final File snapDir, final File logDir) throws IOException
    {
      return new LocalZookeeperServer(snapDir, logDir, tickTime,
          minSessionTimeout, maxSessionTimeout, treeBuilder);
    }
  }
}
