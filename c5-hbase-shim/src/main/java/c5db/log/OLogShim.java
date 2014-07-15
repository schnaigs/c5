/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.log;

import c5db.generated.RegionWalEntry;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.replication.ReplicatorReceipt;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtostuffOutput;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.jetbrains.annotations.NotNull;
import org.jetlang.channels.ChannelSubscription;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A distributed WriteAheadLog using c5's replication algorithm
 */
public class OLogShim implements Syncable, HLog {
  public static final int WAL_SYNC_TIMEOUT_SECONDS = 10;

  private static final Logger LOG = LoggerFactory.getLogger(OLogShim.class);
  private static final int MAX_COMMITS_OUTSTANDING = 10000000;
  private final AtomicLong logSeqNum = new AtomicLong(0);
  private final Replicator replicatorInstance;

  // When logging, place all receipts in this queue. Then, when performing sync, reconcile all
  // queued receipts with received commit notices. To prevent this queue and the commit notice
  // queue from growing without bound, the client of this class must sync regularly. That's
  // something the client would need to do anyway in order to determine whether its writes are
  // succeeding.
  private final BlockingQueue<ListenableFuture<ReplicatorReceipt>> receiptFutureQueue = new LinkedBlockingQueue<>();

  // When the OLogShim's Replicator issues a commit notice, keep track of it in this queue.
  // Then, when performing sync, make use of these notices.
  private final BlockingQueue<IndexCommitNotice> commitNoticeQueue = new ArrayBlockingQueue<>(MAX_COMMITS_OUTSTANDING);

  // Keep track of the most recent IndexCommitNotice that was withdrawn from the commitNoticeQueue.
  private IndexCommitNotice lastCommitNotice;

  /**
   * The caller of this constructor must take responsibility for starting and disposing of
   * the Replicator, and starting and disposing of the fiber.
   */
  public OLogShim(Replicator replicatorInstance, Fiber fiber) {
    this.replicatorInstance = replicatorInstance;
    String quorumId = replicatorInstance.getQuorumId();
    long serverNodeId = replicatorInstance.getId();

    replicatorInstance.getCommitNoticeChannel().subscribe(
        new ChannelSubscription<>(fiber, commitNoticeQueue::add,
            (notice) ->
                notice.nodeId == serverNodeId
                    && notice.quorumId.equals(quorumId)));
  }

  //TODO fix so we don't always insert a huge amount of data
  @Override
  public Long startCacheFlush(byte[] encodedRegionName) {
    return (long) 0;
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    LOG.error("completeCache");
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    LOG.error("abort");
  }

  @Override
  public boolean isLowReplicationRollEnabled() {
    return false;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    return 0;
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    LOG.error("unsupported registerWALActionsListener, NOOP (probably causing bugs!)");
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    LOG.error("unsupported unregisterWALActionsListener, returning false (probably causing bugs!)");
    return false;
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return null;
  }

  @Override
  public byte[][] rollWriter() throws IOException {
    // TODO this is not passed through to underlying OLog implementation.
    return null;
  }

  @Override
  public byte[][] rollWriter(boolean force)
      throws IOException {
    return rollWriter();
  }

  @Override
  public void close() throws IOException {
    // TODO take this as a clue to turn off the ReplicationInstance we depend on.
  }

  @Override
  public void closeAndDelete() throws IOException {
    // TODO still need more info to make this reasonably successful.
    close();
    // TODO can't delete at this level really.  Maybe we need to mark
  }

  @Override
  public void hflush() throws IOException {
    this.sync();
  }

  @Override
  public void hsync() throws IOException {
    this.sync();
  }

  @Override
  public void sync(long txid) throws IOException {
    this.sync();
  }

  @Override
  public void sync() throws IOException {
    // TODO how should this handle a case where some writes succeed and others fail?
    // TODO currently it throws an exception, but that can lead to the appearance that a successful write failed
    List<ListenableFuture<ReplicatorReceipt>> receiptList = new ArrayList<>();
    receiptFutureQueue.drainTo(receiptList);

    try {
      for (ListenableFuture<ReplicatorReceipt> receiptFuture : receiptList) {
        ReplicatorReceipt receipt = receiptFuture.get(WAL_SYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        IndexCommitNotice commitNotice = waitForCommitNotice(receipt.seqNum);
        if (commitNotice.term != receipt.term) {
          throw new IOException("OLogShim#sync: term returned by logData differs from term of IndexCommitNotice");
        }
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException("Error encountered while waiting within OLogShim#sync", e);
    }
  }

  @Override
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  @Override
  // TODO this is a problematic call because the replication algorithm is in charge of the log-ids. We must
  // TODO  depreciate this call, and bubble the consequences throughout the system.
  public void setSequenceNumber(long newValue) {
    for (long id = this.logSeqNum.get(); id < newValue &&
        !this.logSeqNum.compareAndSet(id, newValue); id = this.logSeqNum.get()) {
      LOG.debug("Changed sequenceId from " + id + " to " + newValue);
    }
  }

  @Override
  public long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }


  @Override
  public void append(HRegionInfo info,
                     TableName tableName,
                     WALEdit edits,
                     long now,
                     HTableDescriptor htd) throws IOException {
    this.appendSync(info, tableName, edits, now, htd);
  }

  @Override
  public void append(HRegionInfo info,
                     TableName tableName,
                     WALEdit edits,
                     long now,
                     HTableDescriptor htd,
                     boolean isInMemstore) throws IOException {
    this.appendSync(info, tableName, edits, now, htd);
  }

  @Override
  public long appendNoSync(HRegionInfo info,
                           TableName tableName,
                           WALEdit edit, List<UUID> clusterIds,
                           long now,
                           HTableDescriptor htd) throws IOException {
    try {
      List<ByteBuffer> entryBytes = serializeWalEdit(info.getRegionNameAsString(), edit);

      // our replicator knows what quorumId/tabletId we are.
      ListenableFuture<ReplicatorReceipt> receiptFuture = replicatorInstance.logData(entryBytes);
      if (receiptFuture == null) {
        throw new IOException("OLogShim: attempting to append a WALEdit to a Replicator not in the leader state");
      }
      receiptFutureQueue.add(receiptFuture);

    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return 0;
  }

  // TODO XXX passthrough no longer valid, this call does the wrong thing now.
  @Override
  public long getFilenum() {
    return 0;
  }

  private void appendSync(HRegionInfo info,
                          TableName tableName,
                          WALEdit edits,
                          long now,
                          HTableDescriptor htd) throws IOException {
    appendNoSync(info, tableName, edits, null, now, htd);
    this.sync();
  }

  private static List<ByteBuffer> serializeWalEdit(String regionInfo, WALEdit edit) throws IOException {
    final List<ByteBuffer> buffers = new ArrayList<>();

    for (KeyValue keyValue : edit.getKeyValues()) {
      @SuppressWarnings("deprecation")
      final RegionWalEntry entry = new RegionWalEntry(
          regionInfo,
          ByteBuffer.wrap(keyValue.getRow()),
          ByteBuffer.wrap(keyValue.getFamily()),
          ByteBuffer.wrap(keyValue.getQualifier()),
          ByteBuffer.wrap(keyValue.getValue()),
          keyValue.getTimestamp());
      addLengthPrependedEntryBuffersToList(entry, buffers);
    }
    return buffers;
  }

  private static void addLengthPrependedEntryBuffersToList(RegionWalEntry entry, List<ByteBuffer> buffers)
      throws IOException {
    LinkBuffer entryBuffer = new LinkBuffer();
    LowCopyProtostuffOutput lcpo = new LowCopyProtostuffOutput(entryBuffer);
    RegionWalEntry.getSchema().writeTo(lcpo, entry);

    final int length = Ints.checkedCast(lcpo.buffer.size());
    final LinkBuffer lengthBuf = new LinkBuffer().writeVarInt32(length);

    buffers.addAll(lengthBuf.finish());
    buffers.addAll(entryBuffer.finish());
  }

  private IndexCommitNotice waitForCommitNotice(long index) throws IOException, InterruptedException, TimeoutException {
    while (true) {
      if (lastCommitNotice == null || lastCommitNotice.lastIndex < index) {
        lastCommitNotice = pollWithTimeoutException(commitNoticeQueue, WAL_SYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      } else if (index < lastCommitNotice.firstIndex) {
        LOG.error("weird, requested to wait for a notice with index {} but that's prior to the latest commit notice {}",
            index, lastCommitNotice);
        throw new IOException("waitForCommitNotice");

      } else {
        return lastCommitNotice;
      }
    }
  }

  private <T> T pollWithTimeoutException(BlockingQueue<T> queue, long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, TimeoutException {
    T result = queue.poll(timeout, unit);
    if (result == null) {
      throw new TimeoutException("pollWithTimeoutException");
    }
    return result;
  }
}