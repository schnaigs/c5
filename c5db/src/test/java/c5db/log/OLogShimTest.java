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

import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.replication.ReplicatorReceipt;
import c5db.tablet.SystemTableNames;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static c5db.FutureActions.returnFutureWithValue;

public class OLogShimTest {
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private final Replicator replicator = context.mock(Replicator.class);
  private final HTableDescriptor descriptor = SystemTableNames.rootTableDescriptor();
  private final HRegionInfo info = SystemTableNames.rootRegionInfo();
  private final TableName tableName = descriptor.getTableName();

  private final Channel<IndexCommitNotice> commitNoticeChannel = new MemoryChannel<>();
  private final Fiber oLogShimFiber = new ThreadFiber();

  private HLog hLog;

  @Before
  public void setOverallExpectationsAndCreateTestObject() {
    context.checking(new Expectations() {{
      allowing(replicator).getQuorumId();

      allowing(replicator).getId();
      will(returnValue(1L));

      allowing(replicator).getCommitNoticeChannel();
      will(returnValue(commitNoticeChannel));
    }});

    oLogShimFiber.start();
    hLog = new OLogShim(replicator, oLogShimFiber);
  }

  @Test
  public void logsOneReplicationDatumPerSubmittedWALEdit() throws Exception {
    context.checking(new Expectations() {{
      oneOf(replicator).logData(with(anyData()));
    }});

    hLog.appendNoSync(info, tableName, aWalEditWithMultipleKeyValues(), aClusterIdList(), currentTime(), descriptor);
  }

  @Test(expected = IOException.class, timeout = 3000)
  public void syncThrowsAnExceptionIfACommitNoticeIsPublishedWhoseTermDisagreesWithTheTermOfALoggedEntry()
      throws Exception {

    havingAppendedAndReceivedReceipt(hLog, new ReplicatorReceipt(term(17), index(13)));

    theReplicatorHavingIssued(new IndexCommitNotice(replicator.getId(), index(13), index(13), term(18)));

    hLog.sync(); // exception
  }

  @Test(timeout = 3000)
  public void syncCanWaitForSeveralPrecedingLogAppends() throws Exception {

    havingAppendedAndReceivedReceipt(hLog, new ReplicatorReceipt(term(101), index(1)));
    havingAppendedAndReceivedReceipt(hLog, new ReplicatorReceipt(term(102), index(2)));
    havingAppendedAndReceivedReceipt(hLog, new ReplicatorReceipt(term(102), index(3)));

    theReplicatorHavingIssued(new IndexCommitNotice(replicator.getId(), 1, 1, 101));
    theReplicatorHavingIssued(new IndexCommitNotice(replicator.getId(), 2, 3, 102));

    hLog.sync();
  }


  private void havingAppendedAndReceivedReceipt(HLog hLog, ReplicatorReceipt receipt) throws Exception {
    context.checking(new Expectations() {{
      allowing(replicator).logData(with(anyData()));
      will(returnFutureWithValue(
          receipt));
    }});

    hLog.appendNoSync(info, tableName, aWalEditWithMultipleKeyValues(), aClusterIdList(), currentTime(), descriptor);
  }

  private void theReplicatorHavingIssued(IndexCommitNotice notice) {
    oLogShimFiber.execute(() ->
        commitNoticeChannel.publish(notice));
  }

  private WALEdit aWalEditWithMultipleKeyValues() {
    WALEdit edit = new WALEdit();

    for (int i = 0; i < 3; i++) {
      byte[] row = new byte[4];
      byte[] family = new byte[1];
      byte[] qualifier = new byte[128];
      byte[] value = new byte[128];
      edit.add(new KeyValue(row, family, qualifier, value));
    }
    return edit;
  }

  private List<UUID> aClusterIdList() {
    return new ArrayList<>();
  }

  private long currentTime() {
    return System.currentTimeMillis();
  }

  private Matcher<List<ByteBuffer>> anyData() {
    return Matchers.instanceOf(List.class);
  }

  private long term(long term) {
    return term;
  }

  private long index(long index) {
    return index;
  }
}
