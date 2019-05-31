/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.impl;

import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.BackupInput;
import io.atomix.primitive.service.BackupOutput;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.protocols.raft.impl.RaftContext;
import io.atomix.protocols.raft.service.RaftServiceContext;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.distributedlog.DistributedLogstreamClient;
import io.zeebe.distributedlog.DistributedLogstreamService;
import io.zeebe.distributedlog.DistributedLogstreamType;
import io.zeebe.distributedlog.StorageConfiguration;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreFactory;
import io.zeebe.distributedlog.restore.RestoreNodeProvider;
import io.zeebe.distributedlog.restore.RestoreStrategy;
import io.zeebe.distributedlog.restore.impl.DefaultStrategyPicker;
import io.zeebe.distributedlog.restore.log.LogReplicationAppender;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.distributedlog.restore.snapshot.RestoreSnapshotReplicator;
import io.zeebe.distributedlog.restore.snapshot.SnapshotConsumer;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreContext;
import io.zeebe.distributedlog.restore.snapshot.impl.SnapshotConsumerImpl;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.impl.service.LogStreamServiceNames;
import io.zeebe.logstreams.log.BufferedLogStreamReader;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.spi.LogStorage;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.servicecontainer.ServiceContainer;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDistributedLogstreamService
    extends AbstractPrimitiveService<DistributedLogstreamClient>
    implements DistributedLogstreamService, LogReplicationAppender {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultDistributedLogstreamService.class);

  private LogStream logStream;
  private LogStorage logStorage;
  private String logName;
  private int partitionId;
  private String currentLeader;
  private long currentLeaderTerm = -1;
  private long lastPosition;
  private ServiceContainer serviceContainer;
  private ThreadContext restoreThreadContext;
  private String localMemberId;

  public DefaultDistributedLogstreamService(DistributedLogstreamServiceConfig config) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
    lastPosition = -1;
  }

  public DefaultDistributedLogstreamService(LogStream logStream, String localMemberId) {
    super(DistributedLogstreamType.instance(), DistributedLogstreamClient.class);
    this.logStream = logStream;
    this.logStorage = this.logStream.getLogStorage();
    this.logName = logStream.getLogName();
    restoreThreadContext = new SingleThreadContext(String.format("log-restore-%s-%%d", logName));
    this.localMemberId = localMemberId;

    initLastPosition();
  }

  @Override
  protected void configure(ServiceExecutor executor) {
    super.configure(executor);
    try {
      logName = getRaftPartitionName(executor);
      restoreThreadContext = new SingleThreadContext(String.format("log-restore-%s-%%d", logName));
      LOG.info(
          "Configuring {} on node {} with logName {}",
          getServiceName(),
          getLocalMemberId().id(),
          logName);

      createLogStream(logName);
    } catch (Exception e) {
      LOG.error(
          "Failed to configure {} on node {} with logName {}",
          getServiceName(),
          getLocalMemberId().id(),
          logName,
          e);
      throw e;
    }
  }

  private String getRaftPartitionName(ServiceExecutor executor) {
    final String name;

    try {
      final Field context = DefaultServiceExecutor.class.getDeclaredField("context");
      context.setAccessible(true);
      final RaftServiceContext raftServiceContext = (RaftServiceContext) context.get(executor);
      final Field raft = RaftServiceContext.class.getDeclaredField("raft");
      raft.setAccessible(true);
      final RaftContext raftContext = (RaftContext) raft.get(raftServiceContext);
      name = raftContext.getName();
      raft.setAccessible(false);
      context.setAccessible(false);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    return name;
  }

  private void createLogStream(String logServiceName) {
    // A hack to get partitionId from the name
    final String[] splitted = logServiceName.split("-");
    partitionId = Integer.parseInt(splitted[splitted.length - 1]);

    localMemberId = getLocalMemberId().id();
    serviceContainer = LogstreamConfig.getServiceContainer(localMemberId);

    if (serviceContainer.hasService(LogStreamServiceNames.logStreamServiceName(logServiceName))) {
      logStream = LogstreamConfig.getLogStream(localMemberId, partitionId);
    } else {

      final StorageConfiguration config =
          LogstreamConfig.getConfig(localMemberId, partitionId).join();

      final File logDirectory = config.getLogDirectory();
      final File snapshotDirectory = config.getSnapshotsDirectory();
      final File blockIndexDirectory = config.getBlockIndexDirectory();

      final StateStorage stateStorage = new StateStorage(blockIndexDirectory, snapshotDirectory);
      logStream =
          LogStreams.createFsLogStream(partitionId)
              .logDirectory(logDirectory.getAbsolutePath())
              .logSegmentSize((int) config.getLogSegmentSize())
              .indexBlockSize((int) config.getIndexBlockSize())
              .logName(logServiceName)
              .serviceContainer(serviceContainer)
              .indexStateStorage(stateStorage)
              .build()
              .join();
    }

    LogstreamConfig.putLogStream(localMemberId, partitionId, logStream);

    this.logStorage = this.logStream.getLogStorage();
    initLastPosition();

    LOG.info("Logstreams created. last appended event at position {}", lastPosition);
  }

  private void initLastPosition() {
    final BufferedLogStreamReader reader = new BufferedLogStreamReader(logStream);
    reader.seekToLastEvent();
    lastPosition =
        Math.max(
            logStream.getCommitPosition(),
            reader.getPosition()); // position of last event which is committed
  }

  @Override
  public long append(String nodeId, long commitPosition, byte[] blockBuffer) {
    // Assumption: first append is always called after claim leadership. So currentLeader is not
    // null. Assumption is also valid during restart.
    if (!currentLeader.equals(nodeId)) {
      LOG.warn(
          "Append request from follower node {}. Current leader is {}.", nodeId, currentLeader);
      return 0; // Don't return a error, so that the appender never retries. TODO: Return a proper
      // error code;
    }

    return append(commitPosition, blockBuffer);
  }

  @Override
  public long append(long commitPosition, byte[] blockBuffer) {
    if (commitPosition <= lastPosition) {
      // This case can happen due to raft-replay or when appender retries due to timeout or other
      // exceptions.
      LOG.trace("Rejecting append request at position {}", commitPosition);
      return 1; // Assume the append was successful because event was previously appended.
    }

    final ByteBuffer buffer = ByteBuffer.wrap(blockBuffer);
    final long appendResult = logStorage.append(buffer);
    if (appendResult > 0) {
      updateCommitPosition(commitPosition);
    } else {
      LOG.error("Append failed {}", appendResult);
    }
    // the return result is valid only for the leader. If the followers failed to append, they don't
    // retry
    return appendResult;
  }

  @Override
  public boolean claimLeaderShip(String nodeId, long term) {
    LOG.debug(
        "Node {} claiming leadership for logstream partition {} at term {}.",
        nodeId,
        logStream.getPartitionId(),
        term);

    if (currentLeaderTerm < term) {
      this.currentLeader = nodeId;
      this.currentLeaderTerm = term;
      return true;
    }
    return false;
  }

  @Override
  public void backup(BackupOutput backupOutput) {
    // This doesn't back up the events in the logStream. All the log entries occurred before the
    // backup snapshot, but not appended to logStorage may be lost. So, if this node is away for a
    // while and tries to recover with backup received from other nodes, there will be missing
    // entries in the logStorage.

    LOG.info("Backup log {} at position {}", logName, lastPosition);
    // Backup in-memory states
    backupOutput.writeLong(lastPosition);
    backupOutput.writeString(currentLeader);
    backupOutput.writeLong(currentLeaderTerm);
  }

  @Override
  public void restore(BackupInput backupInput) {
    final long backupPosition = backupInput.readLong();
    restore(backupPosition);

    LOG.debug("Restored local log to position {}", lastPosition);
    currentLeader = backupInput.readString();
    currentLeaderTerm = backupInput.readLong();
  }

  public void restore(long backupPosition) {
    if (lastPosition < backupPosition) {
      LogstreamConfig.startRestore(localMemberId, partitionId);
      final long latestLocalPosition = lastPosition;

      // pick the correct restore strategy and execute forever until the log is restored, e.g.
      // lastPosition >= backupPosition.
      while (lastPosition < backupPosition) {
        LOG.debug("Restoring local log from position {} to {}", lastPosition, backupPosition);
        try {
          final DefaultStrategyPicker strategyPicker = this.buildRestoreStrategyPicker();
          final long lastUpdatedPosition =
              strategyPicker
                  .pick(latestLocalPosition, backupPosition)
                  .thenCompose(RestoreStrategy::executeRestoreStrategy)
                  .join();
          updateCommitPosition(lastUpdatedPosition);
          LOG.trace("Restored local log from position {} to {}", latestLocalPosition, lastPosition);
        } catch (RuntimeException e) {
          lastPosition = logStream.getCommitPosition();
          LOG.error(
              "Failed to restore log from {} to {}, retrying from {}",
              latestLocalPosition,
              backupPosition,
              lastPosition,
              e);
        }
      }
      LogstreamConfig.completeRestore(localMemberId, partitionId);
    }
  }

  private void updateCommitPosition(long commitPosition) {
    // Following is required to trigger the commit listeners.
    logStream.setCommitPosition(commitPosition);
    lastPosition = commitPosition;
  }

  private DefaultStrategyPicker buildRestoreStrategyPicker() {
    final RestoreFactory clientFactory = LogstreamConfig.getRestoreFactory(localMemberId);
    final RestoreClient restoreClient = clientFactory.createClient(partitionId);
    final LogReplicator logReplicator =
        new LogReplicator(this, restoreClient, restoreThreadContext, LOG);

    final SnapshotRestoreContext snapshotRestoreContext =
        clientFactory.createSnapshotRestoreContext();
    final StateStorage storage = snapshotRestoreContext.getStateStorage(partitionId);
    final SnapshotConsumer snapshotConsumer = new SnapshotConsumerImpl(storage, LOG);
    final RestoreSnapshotReplicator snapshotReplicator =
        new RestoreSnapshotReplicator(
            restoreClient,
            snapshotRestoreContext,
            snapshotConsumer,
            storage,
            partitionId,
            restoreThreadContext,
            LOG);

    final RestoreNodeProvider nodeProvider = clientFactory.createNodeProvider(partitionId);

    return new DefaultStrategyPicker(
        restoreClient, nodeProvider, logReplicator, snapshotReplicator, restoreThreadContext, LOG);
  }

  @Override
  public void close() {
    super.close();
    LOG.info("Closing {}", getServiceName());
  }
}
