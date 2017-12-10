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

import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

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

  @Override
  public void shutdown()
  {
    super.shutdown();

    try {
      this.getZKDatabase().close();
      this.getTxnLogFactory().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
