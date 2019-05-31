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
package io.zeebe.distributedlog.restore.snapshot.impl;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

import io.zeebe.distributedlog.restore.snapshot.SnapshotConsumer;
import io.zeebe.logstreams.state.SnapshotChunk;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import org.slf4j.Logger;

public class SnapshotConsumerImpl implements SnapshotConsumer {

  private final StateStorage stateStorage;
  private final Logger logger;

  public SnapshotConsumerImpl(StateStorage stateStorage, Logger logger) {
    this.stateStorage = stateStorage;
    this.logger = logger;
  }

  @Override
  public boolean consumeSnapshotChunk(SnapshotChunk chunk) {
    return writeChunkToDisk(chunk, stateStorage);
  }

  @Override
  public boolean markSnapshotValid(long snapshotId) {
    return tryToMarkSnapshotAsValid(stateStorage, snapshotId);
  }

  @Override
  public boolean clearTmpSnapshot(long snapshotId) {

    final File tmpSnapshotDirectory =
        stateStorage.getTmpSnapshotDirectoryFor(Long.toString(snapshotId));
    try {
      if (tmpSnapshotDirectory.exists()) {
        FileUtil.deleteFolder(tmpSnapshotDirectory.toPath());
      }
    } catch (IOException e) {

    }
    return true;
  }

  // TODO: following methods copied from ReplicationController
  private static long createChecksum(byte[] content) {
    final CRC32 crc32 = new CRC32();
    crc32.update(content);
    return crc32.getValue();
  }

  private boolean writeChunkToDisk(SnapshotChunk snapshotChunk, StateStorage storage) {
    final long snapshotPosition = snapshotChunk.getSnapshotPosition();
    final String snapshotName = Long.toString(snapshotPosition);
    final String chunkName = snapshotChunk.getChunkName();

    if (storage.existSnapshot(snapshotPosition)) {
      return true;
    }

    final long expectedChecksum = snapshotChunk.getChecksum();
    final long actualChecksum = createChecksum(snapshotChunk.getContent());

    if (expectedChecksum != actualChecksum) {
      return false;
    }

    final File tmpSnapshotDirectory = storage.getTmpSnapshotDirectoryFor(snapshotName);
    if (!tmpSnapshotDirectory.exists()) {
      tmpSnapshotDirectory.mkdirs();
    }

    final File snapshotFile = new File(tmpSnapshotDirectory, chunkName);
    if (snapshotFile.exists()) {
      logger.debug("Received a snapshot file which already exist '{}'.", snapshotFile);
      return false;
    }

    logger.debug("Consume snapshot chunk {}", chunkName);
    return writeReceivedSnapshotChunk(snapshotChunk, snapshotFile);
  }

  private boolean writeReceivedSnapshotChunk(SnapshotChunk snapshotChunk, File snapshotFile) {
    try {
      Files.write(
          snapshotFile.toPath(), snapshotChunk.getContent(), CREATE_NEW, StandardOpenOption.WRITE);
      logger.debug("Wrote replicated snapshot chunk to file {}", snapshotFile.toPath());
      return true;
    } catch (IOException ioe) {
      logger.error(
          "Unexpected error occurred on writing an snapshot chunk to '{}'.", snapshotFile, ioe);
      return false;
    }
  }

  private boolean tryToMarkSnapshotAsValid(StateStorage storage, long snapshotId) {

    final File validSnapshotDirectory = storage.getSnapshotDirectoryFor(snapshotId);

    final File tmpSnapshotDirectory = storage.getTmpSnapshotDirectoryFor(Long.toString(snapshotId));

    try {
      Files.move(tmpSnapshotDirectory.toPath(), validSnapshotDirectory.toPath());
      return true;
    } catch (FileAlreadyExistsException e) {
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }
}
