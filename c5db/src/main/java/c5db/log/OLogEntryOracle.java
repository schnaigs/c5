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

import c5db.replication.QuorumConfiguration;

/**
 * Keeps track of, and provides answers about, logged OLogEntries.
 */
public interface OLogEntryOracle {
  /**
   * Accept an OLogEntry and possibly incorporate it into the information tracked.
   * This method must be called for every OLogEntry logged, or else the OLogEntryOracle
   * will be out of sync with the log.
   *
   * @param entry Entry being logged.
   */
  void notifyLogging(OLogEntry entry);

  /**
   * This method removes information from the map. It must be called when the log
   * has truncated some entries.
   *
   * @param seqNum Index to truncate back to, inclusive.
   */
  void notifyTruncation(long seqNum);

  /**
   * Get the log term for a specified sequence number, or zero if the sequence number
   * is less than that of every entry logged.
   *
   * @param seqNum Log sequence number
   * @return The log term at this sequence number
   */
  long getTermAtSeqNum(long seqNum);

  /**
   * Get the quorum configuration which was active as of a specified sequence number,
   * together with the sequence number at which it was established. If the sequence
   * number is less than every entry logged, return the empty quorum configuration and
   * a seqNum of zero.
   *
   * @param seqNum Log sequence number
   * @return The quorum configuration active at this sequence number, and the sequence
   * number at which it became active.
   */
  QuorumConfigurationWithSeqNum getConfigAtSeqNum(long seqNum);


  interface OLogEntryOracleFactory {
    OLogEntryOracle create();
  }

  class QuorumConfigurationWithSeqNum {
    public final QuorumConfiguration quorumConfiguration;
    public final long seqNum;

    public QuorumConfigurationWithSeqNum(QuorumConfiguration quorumConfiguration, long seqNum) {
      this.quorumConfiguration = quorumConfiguration;
      this.seqNum = seqNum;
    }

    @Override
    public String toString() {
      return "QuorumConfigurationWithSeqNum{" +
          "quorumConfiguration=" + quorumConfiguration +
          ", seqNum=" + seqNum +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      QuorumConfigurationWithSeqNum that = (QuorumConfigurationWithSeqNum) o;

      return seqNum == that.seqNum
          && quorumConfiguration.equals(that.quorumConfiguration);
    }

    @Override
    public int hashCode() {
      int result = quorumConfiguration != null ? quorumConfiguration.hashCode() : 0;
      result = 31 * result + (int) (seqNum ^ (seqNum >>> 32));
      return result;
    }
  }
}

