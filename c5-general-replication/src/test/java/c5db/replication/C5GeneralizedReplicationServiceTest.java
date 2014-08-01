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
import c5db.ReplicatorConstants;
import c5db.discovery.BeaconService;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.log.ReplicatorLogGenericTestUtil;
import c5db.util.C5Futures;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matcher;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static c5db.CollectionMatchers.isStrictlyIncreasing;
import static c5db.FutureMatchers.resultsIn;
import static com.google.common.util.concurrent.Futures.allAsList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class C5GeneralizedReplicationServiceTest {
  @Rule
  public JUnitRuleFiberExceptions jUnitFiberExceptionHandler = new JUnitRuleFiberExceptions();

  private static final int NUMBER_OF_PROCESSORS = Runtime.getRuntime().availableProcessors();

  private final Path baseTestPath = new C5CommonTestUtil().getDataTestDir("general-replicator-test");

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_PROCESSORS);

  private final PoolFiberFactory fiberFactory = new PoolFiberFactory(executorService);
  private final Set<Fiber> fibers = new HashSet<>();
  private final Fiber mainTestFiber = newFiber(jUnitFiberExceptionHandler);

  @Before
  public void startTestFiber() throws Exception {
    mainTestFiber.start();
  }

  @After
  public void disposeOfResources() throws Exception {
    fiberFactory.dispose();
    executorService.shutdownNow();
    fibers.forEach(Fiber::dispose);
  }

  @Test(timeout = 9000)
  public void logsToASingleQuorumReplicator() throws Exception {
    List<Long> peerIds = Lists.newArrayList(1L);

    try (QuorumOfReplicatorsController controller = new QuorumOfReplicatorsController(peerIds, this::newFiber)) {

      GeneralizedReplicator replicator = controller.waitUntilAReplicatorIsReady();

      List<ListenableFuture<Long>> replicateFutures = new ArrayList<ListenableFuture<Long>>() {{
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
      }};

      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(hasSize(3)));
      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(isStrictlyIncreasing()));
    }
  }

  @Test(timeout = 9000)
  public void replicatesAcrossAQuorumComposedOfThreeReplicators() throws Exception {
    List<Long> peerIds = Lists.newArrayList(1L, 2L, 3L);

    try (QuorumOfReplicatorsController controller = new QuorumOfReplicatorsController(peerIds, this::newFiber)) {

      GeneralizedReplicator replicator = controller.waitUntilAReplicatorIsReady();

      List<ListenableFuture<Long>> replicateFutures = new ArrayList<ListenableFuture<Long>>() {{
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
        add(replicator.replicate(someData()));
      }};

      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(hasSize(3)));
      assertThat(allAsList(replicateFutures), resultsInAListOfLongsThat(isStrictlyIncreasing()));
    }
  }

  private Fiber newFiber(Consumer<Throwable> throwableHandler) {
    Fiber newFiber = fiberFactory.create(new ExceptionHandlingBatchExecutor(throwableHandler));
    fibers.add(newFiber);
    return newFiber;
  }

  private List<ByteBuffer> someData() {
    return Lists.newArrayList(ReplicatorLogGenericTestUtil.someData());
  }

  private static Matcher<? super ListenableFuture<List<Long>>> resultsInAListOfLongsThat(
      Matcher<? super List<Long>> longsMatcher) {
    return resultsIn(longsMatcher);
  }

  /**
   * Runs a complete quorum of C5GeneralizedReplicationService and handles startup and disposal,
   * for the purpose of making tests more readable
   */
  private class QuorumOfReplicatorsController implements AutoCloseable {
    private static final String QUORUM_ID = "quorumId";

    private final Collection<Long> peerIds;
    private final FiberSupplier fiberSupplier;
    private final Map<Long, C5GeneralizedReplicationService> services = new HashMap<>();
    private final Map<Long, GeneralizedReplicator> replicators = new HashMap<>();

    public QuorumOfReplicatorsController(Collection<Long> peerIds, FiberSupplier fiberSupplier) throws Exception {
      this.peerIds = peerIds;
      this.fiberSupplier = fiberSupplier;

      createServicesForEachPeerId();
      createReplicatorsForEachService();
    }

    @Override
    public void close() {
      services.values().forEach((service) -> {
        service.stopAndWait();
        service.dispose();
      });
    }

    public GeneralizedReplicator waitUntilAReplicatorIsReady() throws Exception {
      SettableFuture<GeneralizedReplicator> readyReplicatorFuture = SettableFuture.create();

      for (GeneralizedReplicator replicator : replicators.values()) {
        final ListenableFuture<Void> isAvailableFuture = replicator.isAvailableFuture();

        C5Futures.addCallback(isAvailableFuture,
            (ignore) -> readyReplicatorFuture.set(replicator),
            readyReplicatorFuture::setException,
            mainTestFiber);
      }

      return readyReplicatorFuture.get();
    }

    private void createServicesForEachPeerId() {
      int port = ReplicatorConstants.REPLICATOR_PORT_MIN;

      for (long peerId : peerIds) {
        C5GeneralizedReplicationService service =
            new C5GeneralizedReplicationService(baseTestPath, peerId, port, BeaconService::new, fiberSupplier);

        port++;
        service.startAndWait();
        services.put(peerId, service);
      }
    }

    private void createReplicatorsForEachService() throws Exception {
      for (long peerId : peerIds) {
        GeneralizedReplicator replicator = services.get(peerId).createReplicator(QUORUM_ID, peerIds).get();
        replicators.put(peerId, replicator);
      }
    }
  }
}
