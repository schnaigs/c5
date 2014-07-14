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

import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.generated.RequestVote;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.protostuff.LinkBuffer;
import io.protostuff.LowCopyProtobufOutput;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class ReplicationWireMessageTest {

  @Test
  public void testSimpleSerialization() throws Exception {
    RequestVote rv = new RequestVote(1, 22222, 34, 22);
    ReplicationWireMessage rwm = new ReplicationWireMessage(
        1, 1, 0, "quorumId", false, rv, null, null, null, null, null
    );

    LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(new LinkBuffer(24));
    rwm.writeTo(lcpo, rwm);
    List<ByteBuffer> serBufs = lcpo.buffer.finish();
    logBufsInformation("ReplicationWireMessage", serBufs);

    ByteBuf b = Unpooled.wrappedBuffer(serBufs.toArray(new ByteBuffer[serBufs.size()]));
    System.out.println("ByteBuf info = " + b);
    System.out.println("ByteBuf size = " + b.readableBytes());
    assertEquals(lcpo.buffer.size(), b.readableBytes());

    System.out.println("rwm = " + rwm);
  }

  void logBufsInformation(String desc, List<ByteBuffer> buffs) {
    System.out.println(desc + ": buffer count = " + buffs.size());
    long size = 0;
    for (ByteBuffer b : buffs) {
      System.out.println(desc + ": buff=" + b);
      size += b.remaining();
    }
    System.out.println(desc + ": totalSize = " + size);
  }
}
