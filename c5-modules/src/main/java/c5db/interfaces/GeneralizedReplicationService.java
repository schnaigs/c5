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

package c5db.interfaces;

import c5db.interfaces.log.Reader;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.interfaces.replication.ReplicatorEntry;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.Collection;

public interface GeneralizedReplicationService extends Service {

  ListenableFuture<GeneralizedReplicator> createReplicator(String quorumId, Collection<Long> peerIds);

  public Reader<ReplicatorEntry> getLogReader(String quorumId);
}
