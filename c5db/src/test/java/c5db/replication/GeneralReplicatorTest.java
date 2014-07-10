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

package c5db.replication;

import c5db.C5CommonTestUtil;
import c5db.ConfigDirectory;
import c5db.IndexCommitMatcher;
import c5db.NioFileConfigDirectory;
import c5db.discovery.BeaconService;
import c5db.interfaces.C5Module;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.ModuleServer;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.log.LogService;
import c5db.log.LogTestUtil;
import c5db.messages.generated.ModuleType;
import c5db.util.C5Futures;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberOnly;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.Subscriber;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static c5db.AsyncChannelAsserts.ChannelHistoryMonitor;
import static c5db.C5ServerConstants.DISCOVERY_PORT;
import static c5db.C5ServerConstants.REPLICATOR_PORT_MIN;
import static c5db.interfaces.replication.ReplicatorInstanceEvent.EventType.LEADER_ELECTED;
import static c5db.replication.ReplicatorService.FiberFactory;
import static org.hamcrest.Matchers.equalTo;

public class GeneralReplicatorTest {
  @Rule
  public JUnitRuleFiberExceptions jUnitFiberExceptionHandler = new JUnitRuleFiberExceptions();

  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);
  private final EventLoopGroup bossGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);
  private final EventLoopGroup workerGroup = new NioEventLoopGroup(NUMBER_OF_PROCESSORS / 3);

  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(executorService);
  private final Set<Fiber> fibers = new HashSet<>();
  private final Fiber mainTestFiber = newExceptionHandlingFiber(jUnitFiberExceptionHandler);

  private ConfigDirectory configDirectory;

  @Before
  public void setupConfigDirectory() throws Exception {
    configDirectory = new NioFileConfigDirectory(new C5CommonTestUtil().getDataTestDir("general-replicator-test"));
    mainTestFiber.start();
  }

  @After
  public void disposeOfResources() throws Exception {
    fiberFactory.dispose();
    executorService.shutdownNow();
    fibers.forEach(Fiber::dispose);

    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();

    bossGroup.terminationFuture().get();
    workerGroup.terminationFuture().get();
  }

  @Test(timeout = 9000)
  public void establishesASystemForGenericReplicationOfDataAcrossNodes() throws Exception {
    ReplicationServer replicationServer = new ReplicationServer(1, REPLICATOR_PORT_MIN, DISCOVERY_PORT, this::newExceptionHandlingFiber);
    replicationServer.startAndWait();

    Replicator replicator = replicationServer.createReplicator("quorumId", Lists.newArrayList(1L)).get();

    ChannelHistoryMonitor<ReplicatorInstanceEvent> eventMonitor = new ChannelHistoryMonitor<>(replicator.getEventChannel(), mainTestFiber);
    ChannelHistoryMonitor<IndexCommitNotice> commitMonitor = new ChannelHistoryMonitor<>(replicator.getCommitNoticeChannel(), mainTestFiber);

    replicator.start();

    eventMonitor.waitFor(ReplicationMatchers.aReplicatorEvent(LEADER_ELECTED));

    replicator.logData(Lists.newArrayList(LogTestUtil.someData())).get();

    commitMonitor.waitFor(IndexCommitMatcher.aCommitNotice().withIndex(equalTo(1L)));

    replicationServer.stopAndWait();
    replicationServer.dispose();
  }

  private Fiber newExceptionHandlingFiber(Consumer<Throwable> throwableHandler) {
    Fiber newFiber = fiberFactory.create(new ExceptionHandlingBatchExecutor(throwableHandler));
    fibers.add(newFiber);
    return newFiber;
  }

  /**
   * Stand-in for C5Server; coordinates the interaction of the local modules
   */
  private class SimpleModuleServer implements ModuleServer {
    private final Fiber fiber;
    private final Map<ModuleType, C5Module> modules = new HashMap<>();
    private final Map<ModuleType, Integer> modulePorts = new HashMap<>();
    private final Channel<ImmutableMap<ModuleType, Integer>> modulePortsChannel = new MemoryChannel<>();

    private SimpleModuleServer(Fiber fiber) {
      this.fiber = fiber;
    }

    public ListenableFuture<Service.State> startModule(C5Module module) {
      SettableFuture<Service.State> startedFuture = SettableFuture.create();
      Service.Listener stateChangeListener = new SimpleModuleListener(
          module,
          () -> {
            addRunningModule(module);
            startedFuture.set(null);
          },
          () -> removeModule(module));

      module.addListener(stateChangeListener, fiber);
      modules.put(module.getModuleType(), module);
      module.start();

      return startedFuture;
    }

    @Override
    public ListenableFuture<C5Module> getModule(ModuleType moduleType) {
      SettableFuture<C5Module> moduleFuture = SettableFuture.create();
      fiber.execute(() -> moduleFuture.set(modules.get(moduleType)));
      return moduleFuture;
    }

    @Override
    public Subscriber<ImmutableMap<ModuleType, Integer>> availableModulePortsChannel() {
      return modulePortsChannel;
    }

    @Override
    public ImmutableMap<ModuleType, C5Module> getModules()
        throws ExecutionException, InterruptedException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @FiberOnly
    private void addRunningModule(C5Module module) {
      ModuleType type = module.getModuleType();
      if (modules.containsKey(type) && modules.get(type).equals(module)) {
        modulePorts.put(type, module.port());
        publishCurrentActivePorts();
      }
    }

    @FiberOnly
    private void removeModule(C5Module module) {
      ModuleType type = module.getModuleType();
      if (modules.containsKey(type) && modules.get(type).equals(module)) {
        modules.remove(type);
        modulePorts.remove(type);
        publishCurrentActivePorts();
      }
    }

    @FiberOnly
    private void publishCurrentActivePorts() {
      modulePortsChannel.publish(ImmutableMap.copyOf(modulePorts));
    }
  }

  /**
   * Replication server that includes its own discovery and logging mechanism; internally
   * it bundles a ReplicationModule, a LogModule, and a DiscoveryModule. It then implements
   * the ReplicationModule interface and delegates to its internal ReplicationModule.
   */
  private class ReplicationServer extends AbstractService implements ReplicationModule {
    private final Fiber serverFiber;
    private final SimpleModuleServer moduleServer;
    private final Fiber discoveryFiber;
    private final DiscoveryModule discoveryModule;
    private final LogModule logModule = new LogService(configDirectory);
    private final ReplicationModule replicationModule;

    private final int replicatorPort;

    public ReplicationServer(long nodeId, int replicatorPort, int discoveryPort, FiberFactory fiberFactory) {
      this.replicatorPort = replicatorPort;

      serverFiber = fiberFactory.getFiber(jUnitFiberExceptionHandler);
      moduleServer = new SimpleModuleServer(serverFiber);
      serverFiber.start();

      // TODO BeaconService will start this Fiber on its own, but it probably should be started by us
      discoveryFiber = fiberFactory.getFiber(jUnitFiberExceptionHandler);
      discoveryModule = new BeaconService(nodeId, discoveryPort, discoveryFiber, workerGroup, ImmutableMap.of(), moduleServer);

      // TODO ReplicatorService provides no way to dispose of its own fiber; it should
      replicationModule = new ReplicatorService(bossGroup, workerGroup, nodeId, replicatorPort, moduleServer, fiberFactory, configDirectory);
    }

    @Override
    protected void doStart() {
      List<ListenableFuture<Service.State>> startFutures = new ArrayList<>();

      startFutures.add(moduleServer.startModule(logModule));
      startFutures.add(moduleServer.startModule(discoveryModule));
      startFutures.add(moduleServer.startModule(replicationModule));

      ListenableFuture<List<Service.State>> allFutures = Futures.allAsList(startFutures);

      C5Futures.addCallback(allFutures,
          (List<Service.State> ignore) -> notifyStarted(),
          this::notifyFailed,
          serverFiber);
    }

    @Override
    protected void doStop() {
      replicationModule.stopAndWait();
      discoveryModule.stopAndWait();
      logModule.stopAndWait();

      notifyStopped();
    }

    @Override
    public ListenableFuture<Replicator> createReplicator(String quorumId, Collection<Long> peerIds) {
      return replicationModule.createReplicator(quorumId, peerIds);
    }

    @Override
    public ModuleType getModuleType() {
      return ModuleType.Replication;
    }

    @Override
    public boolean hasPort() {
      return true;
    }

    @Override
    public int port() {
      return replicatorPort;
    }

    @Override
    public String acceptCommand(String commandString) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    public void dispose() {
      serverFiber.dispose();

    }
  }

  /**
   * A {@link com.google.common.util.concurrent.Service.Listener} that logs the module lifecycle
   * and performs a single action when the module stops or fails.
   */
  private class SimpleModuleListener implements Service.Listener {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final C5Module module;
    private final Runnable removeModule;
    private final Runnable runningModule;

    private SimpleModuleListener(C5Module module, Runnable runningModule, Runnable removeModule) {
      this.module = module;
      this.removeModule = removeModule;
      this.runningModule = runningModule;
    }

    @Override
    public void starting() {
      logger.info("Started module {}", module);
    }

    @Override
    public void running() {
      logger.info("Running module {}", module);
      runningModule.run();
    }

    @Override
    public void stopping(Service.State from) {
      logger.info("Stopping module {}", module);
      removeModule.run();
    }

    @Override
    public void terminated(Service.State from) {
      logger.info("Terminated module {}", module);
      removeModule.run();
    }

    @Override
    public void failed(Service.State from, Throwable failure) {
      logger.error("Failed module {}: {}", module, failure);
      removeModule.run();
    }
  }
}
