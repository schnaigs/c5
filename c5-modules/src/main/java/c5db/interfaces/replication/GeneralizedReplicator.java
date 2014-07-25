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

import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.util.List;

// TODO this should really be called Replicator, and c5db.interfaces.replication.Replicator should be
// TODO called something like C5Replicator
public interface GeneralizedReplicator {

  /**
   * Replicate data durably. Returns a future which will return when the data is known to
   * have been replicated successfully; or, the future will throw an exception if it is
   * known that a problem occurred replicating the data.
   * <p>
   * Several GeneralizedReplicator instances, possibly hosted on different machines, may
   * cooperate in replicating data.
   * <p>
   * The value of the future, on success, is a sequence number assigned by the replication
   * algorithm. For any two requests (to any two GeneralizedReplicators cooperating in
   * replicating the same sequence of data) if both succeed, then they will have different
   * sequence numbers. Also, if any two requests to the same GeneralizedReplicator instance
   * both succeed, and if one "happens-before" the other, then the earlier request will
   * have a lower sequence number than the second.
   */
  ListenableFuture<Long> replicate(List<ByteBuffer> data)
      throws InterruptedException, InvalidReplicatorStateException;

  /**
   * An exception thrown when attempting to replicate but the GeneralizedReplicator is not
   * accepting replication requests -- perhaps because the replicator must be in a certain
   * state to accept requests (e.g. leader in Raft or proposer in Paxos), and that state
   * might not be known until the time the request is submitted.
   */
  public class InvalidReplicatorStateException extends Exception {
    public InvalidReplicatorStateException(String message) {
      super(message);
    }
  }
}
