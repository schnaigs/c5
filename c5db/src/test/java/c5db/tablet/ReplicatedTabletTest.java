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

package c5db.tablet;

import c5db.AsyncChannelAsserts;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.interfaces.tablet.TabletStateChange;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static c5db.AsyncChannelAsserts.assertEventually;
import static c5db.AsyncChannelAsserts.listenTo;
import static c5db.TabletMatchers.hasMessageWithState;

/**
 * TDD/unit test for tablet.
 */
public class ReplicatedTabletTest {
  @Rule
  public final JUnitRuleMockery context = new JUnitRuleMockery() {{
    setThreadingPolicy(new Synchroniser());
  }};

  private MemoryChannel<Replicator.State> stateChannel;
  private MemoryChannel<ReplicatorInstanceEvent> replicatorStateChangeChannel;

  final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  final Replicator replicator = context.mock(Replicator.class);
  final Region.Creator regionCreator = context.mock(Region.Creator.class);
  final Region region = context.mock(Region.class);
  final C5Server server = context.mock(C5Server.class);

  final SettableFuture<Replicator> future = SettableFuture.create();

  // Value objects for the test.
  final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);
  final HRegionInfo regionInfo = new HRegionInfo(TableName.valueOf("tablename"));
  final String regionName = regionInfo.getRegionNameAsString();
  final HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("tablename"));

  final Path path = Paths.get("/");
  final Configuration conf = new Configuration();

  final Fiber tabletFiber = new ThreadFiber();
  ReplicatedTablet replicatedTablet = new ReplicatedTablet(server,
      regionInfo,
      tableDescriptor,
      peerList,
      path,
      conf,
      tabletFiber,
      replicationModule,
      regionCreator);

  AsyncChannelAsserts.ChannelListener<TabletStateChange> tabletStateChannelListener;

  @Before
  public void setup() throws Exception {
    Fiber tabletFiber = new ThreadFiber();
    this.replicatedTablet = new ReplicatedTablet(server,
        regionInfo,
        tableDescriptor,
        peerList,
        path,
        conf,
        tabletFiber,
        replicationModule,
        regionCreator);
    tabletStateChannelListener = listenTo(replicatedTablet.getStateChangeChannel());

    future.set(replicator);
    tabletStateChannelListener = listenTo(replicatedTablet.getStateChangeChannel());
    stateChannel= new MemoryChannel<>();
    replicatorStateChangeChannel = new MemoryChannel<>();

    context.checking(new Expectations() {
      {
        States state = context.states("start");

        allowing(replicator).getQuorumId();
        will(returnValue(regionName));

        oneOf(replicationModule).createReplicator(regionName, peerList);
        will(returnValue(future));
        then(state.is("opening"));

        oneOf(replicator).start();
        when(state.is("opening"));

        oneOf(regionCreator).getHRegion(
            with(any(Path.class)),
            with(equal(regionInfo)),
            with(equal(tableDescriptor)),
            with(any(HLog.class)),
            with(same(conf)));
        will(returnValue(region));
        then(state.is("opened"));
        stateChannel= new MemoryChannel<>();
        replicatorStateChangeChannel = new MemoryChannel<ReplicatorInstanceEvent>();
      }
    });

    context.checking(new Expectations() {{
      allowing(replicator).getStateChannel();
      will(returnValue(stateChannel));

      allowing(replicator).getStateChangeChannel();
      will(returnValue(replicatorStateChangeChannel));

      allowing(replicator).getCommitNoticeChannel();
    }});
  }

  @After
  public void after() {
    tabletFiber.dispose();
    tabletStateChannelListener.dispose();
  }

  @Test
  public void basicTest() throws Throwable {
    replicatedTablet.start();
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));
  }

  @Test
  public void shouldRunCallCallbackWhenTabletBecomesTheLeader() throws Throwable {
    replicatedTablet.start();
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Open));
    stateChannel.publish(Replicator.State.LEADER);
    assertEventually(tabletStateChannelListener, hasMessageWithState(c5db.interfaces.tablet.Tablet.State.Leader));

  }
}