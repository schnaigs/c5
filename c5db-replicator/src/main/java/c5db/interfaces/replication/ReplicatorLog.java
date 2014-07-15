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

package c5db.interfaces.replication;

import c5db.replication.generated.LogEntry;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * A log abstraction for the consensus/replication algorithm. This interface provides logging service for
 * a single replicator; so, each replicator can envision that it has its own log, according to the log
 * abstraction defined in the consensus algorithm.
 * <p>
 * As a replicator I promise not to call from more than 1 thread.
 */
public interface ReplicatorLog {
  /**
   * Log entries to the log.  The entries are in order, and should start from getLastIndex() + 1.
   * <p>
   * The implementation should feel free to verify this.
   * <p>
   * After this call returns, the log implementation should mark these entries as "to be committed"
   * and calls to 'getLastIndex()' should return the last entry in "entries".  The implementation will
   * then return a future that will be set true upon successful sync to durable storage, or else
   * set with an exception.
   * <p>
   * Note that over time, multiple calls to logEntries() may be issued before the prior call has signaled
   * full sync to the client.  This also implies that once this call returns, calls to the other methods
   * of this interface must now return data from these entries.  For example calling getLogTerm(long) should
   * return data from these entries even if they haven't been quite sync'ed to disk yet.
   *
   * @param entries new log entries
   * @return an future that indicates success.
   */
  ListenableFuture<Boolean> logEntries(List<LogEntry> entries);

  /**
   * Get a future which will return the entries in a specified range of indexes from start, inclusive, to end,
   * exclusive. If start and end are equal, the returned list will be empty.
   *
   * @param start the index of the low endpoint of the range (inclusive)
   * @param end   the index of the high endpoint of the range (exclusive)
   * @return a future which will yield the requested entries, or an exception if an error occurs.
   */
  ListenableFuture<List<LogEntry>> getLogEntries(long start, long end);

  /**
   * Get the term for a given log index. If the given index is not present in the log, then this
   * will return 0. A term value of 0 should be considered invalid. This is expected to be fast,
   * so it's a synchronous interface.
   *
   * @param index the index to look up the term for
   * @return the term value at 'index', or 0 if no such entry
   */
  long getLogTerm(long index);

  /**
   * Gets the term value for the last entry in the log. if the log is empty, then this will return
   * 0. A term value of 0 should never be valid.
   *
   * @return the last term or 0 if no such entry
   */
  long getLastTerm();

  /**
   * Gets the index of the most recent log entry.  An index is like a log sequence number, but there are
   * no holes.
   *
   * @return the index or 0 if the log is empty. This implies log entries start at 1.
   */
  long getLastIndex();

  /**
   * Delete all log entries after and including the specified index.
   * <p>
   * To persist the deletion, this might take a few so use a future.
   *
   * @param entryIndex the index entry to truncate log from.
   * @return A future set to true upon completion, or set with an exception upon failure.
   */
  ListenableFuture<Boolean> truncateLog(long entryIndex);

  /**
   * Get the latest, and thus current, configuration of the replicator's peers. (The configuration is
   * logged just like any other entry.)
   *
   * @return The latest configuration; or if there is none, an empty configuration.
   */
  QuorumConfiguration getLastConfiguration();

  /**
   * Get the index of the entry with the latest configuration of the replicator's peers.
   *
   * @return The greatest index of any configuration entry in the log, or zero if there is none.
   */
  long getLastConfigurationIndex();
}
