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

import c5db.C5CommonTestUtil;
import c5db.interfaces.ReplicatorLog;
import c5db.replication.QuorumConfiguration;
import c5db.util.CheckedConsumer;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.collect.Lists;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.Executors;

import static c5db.FutureMatchers.resultsIn;
import static c5db.log.LogMatchers.aListOfEntriesWithConsecutiveSeqNums;
import static c5db.log.LogTestUtil.entries;
import static c5db.log.LogTestUtil.makeSingleEntryList;
import static c5db.log.LogTestUtil.seqNum;
import static c5db.log.LogTestUtil.someConsecutiveEntries;
import static c5db.log.LogTestUtil.someData;
import static c5db.log.LogTestUtil.term;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test the chain Mooring -> QuorumDelegatingLog -> EncodedSequentialLog -> (disk), ensuring that
 * persisted information can be reconstructed from the information left on disk, so that a replicator
 * can pick up where it left off after a crash.
 */
public class LogIntegratedPersistenceTest {
  private static final String QUORUM_ID = "LogIntegratedPersistenceTest";
  private static final int NUM_THREADS = 3;

  private final Path testDirectory = (new C5CommonTestUtil()).getDataTestDir("olog");

  @Test
  public void anEmptyLogReturnsDefaultValuesForIndexesAndTermsAndConfigurations() throws Exception {
    withReplicatorLog((log) -> {
      assertThat(log.getLastIndex(), is(equalTo(0L)));
      assertThat(log.getLastTerm(), is(equalTo(0L)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(0L)));
      assertThat(log.getLastConfiguration(), is(equalTo(QuorumConfiguration.EMPTY)));
    });
  }

  @Test
  public void termAndIndexInformationIsPersisted() throws Exception {
    withReplicatorLog((log) -> {
      log.logEntries(
          entries()
              .term(77).indexes(1, 2)
              .term(78).indexes(3, 4).build());

    });

    withReplicatorLog((log) -> {
      assertThat(log.getLastIndex(), is(equalTo(4L)));
      assertThat(log.getLastTerm(), is(equalTo(78L)));

      assertThat(log.getLogTerm(3), is(equalTo(78L)));
      assertThat(log.getLogTerm(2), is(equalTo(77L)));
    });
  }

  @Test
  public void configurationInformationIsPersisted() throws Exception {
    final QuorumConfiguration firstConfig = QuorumConfiguration
        .of(Lists.newArrayList(1L, 2L, 3L))
        .getTransitionalConfiguration(Lists.newArrayList(2L, 3L, 4L));
    final QuorumConfiguration secondConfig = firstConfig.getCompletedConfiguration();

    withReplicatorLog((log) -> {
      log.logEntries(
          entries()
              .term(97).indexes(1, 2)
              .term(98).configurationAndIndex(firstConfig, 3)
              .term(99).indexes(4, 5)
              .term(99).configurationAndIndex(secondConfig, 6).build());

    });

    withReplicatorLog((log) -> {
      assertThat(log.getLastConfiguration(), is(equalTo(secondConfig)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(6L)));

      log.truncateLog(seqNum(4));

      assertThat(log.getLastConfiguration(), is(equalTo(firstConfig)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(3L)));

      log.truncateLog(seqNum(2));

      assertThat(log.getLastConfiguration(), is(equalTo(QuorumConfiguration.EMPTY)));
      assertThat(log.getLastConfigurationIndex(), is(equalTo(0L)));
    });
  }

  @Test(expected = RuntimeException.class)
  public void informationAboutWhatSequenceNumberComesNextIsPersistedSoThatAnIncorrectSeqNumCanBeCaught()
      throws Exception {
    withReplicatorLog((log) -> {
      log.logEntries(
          entries().term(1).indexes(1, 2, 3).build());
    });

    withReplicatorLog((log) -> {
      log.logEntries(
          entries().term(2).indexes(5, 6, 7).build());
    });
  }

  @Test(timeout = 3000)
  public void headerInformationIsPersistedAfterARollOperation() throws Exception {
    final QuorumConfiguration configuration = QuorumConfiguration.of(Lists.newArrayList(1L, 2L, 3L));
    final long lastTerm = 97;

    try (OLog oLog = getOLog()) {
      new Mooring(oLog, QUORUM_ID)
          .logEntries(entries()
              .term(lastTerm)
              .configurationAndIndex(configuration, 1).build());
      oLog.roll(QUORUM_ID).get();
    }

    withReplicatorLog((log) -> {
      assertThat(log.getLastTerm(), is(equalTo(lastTerm)));
      assertThat(log.getLastConfiguration(), is(equalTo(configuration)));
    });
  }

  @Test(timeout = 3000)
  public void rollsKeepingTrackOfTermCorrectly() throws Exception {
    withOpenOLog((oLog) -> {
      oLog.logEntry(makeSingleEntryList(seqNum(1), term(17), someData()), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntry(makeSingleEntryList(seqNum(2), term(56), someData()), QUORUM_ID);
    });

    withOpenOLog((oLog) -> {
      assertThat(oLog.getLastTerm(QUORUM_ID), Is.is(equalTo(56L)));

      oLog.truncateLog(seqNum(2), QUORUM_ID);
      assertThat(oLog.getLastTerm(QUORUM_ID), Is.is(equalTo(17L)));
    });
  }

  @Test(timeout = 3000)
  public void performsMultiLogFetchesCorrectlyFromPersistence() throws Exception {
    withOpenOLog((oLog) -> {
      oLog.logEntry(someConsecutiveEntries(1, 6), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntry(someConsecutiveEntries(6, 11), QUORUM_ID);
      oLog.roll(QUORUM_ID);
      oLog.logEntry(someConsecutiveEntries(11, 16), QUORUM_ID);
    });

    withOpenOLog((oLog) ->
        assertThat(oLog.getLogEntries(3, 15, QUORUM_ID), resultsIn(aListOfEntriesWithConsecutiveSeqNums(3, 15))));

    withOpenOLog((oLog) ->
        assertThat(oLog.getLogEntries(1, 6, QUORUM_ID), resultsIn(aListOfEntriesWithConsecutiveSeqNums(1, 6))));
  }

  private void withReplicatorLog(CheckedConsumer<ReplicatorLog, Exception> useLog) throws Exception {
    try (OLog oLog = getOLog()) {
      useLog.accept(new Mooring(oLog, QUORUM_ID));
    }
  }

  private void withOpenOLog(CheckedConsumer<OLog, Exception> useLog) throws Exception {
    try (OLog oLog = getOLog()) {
      oLog.openAsync(QUORUM_ID).get();
      useLog.accept(oLog);
    }
  }

  private OLog getOLog() throws Exception {
    return new QuorumDelegatingLog(
        new LogFileService(testDirectory),
        new WrappingKeySerializingExecutor(Executors.newFixedThreadPool(NUM_THREADS)),
        NavigableMapOLogEntryOracle::new,
        InMemoryPersistenceNavigator::new);
  }

}
