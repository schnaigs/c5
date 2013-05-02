/*
 * Copyright (C) 2013  Ohm Data
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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

/*
 * Copyright (c) 2009-2010 by Bjoern Kolbeck, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */

package org.xtreemfs.foundation.flease.comm.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 *
 * @author bjko
 */
public interface NIOServer {

    /**
     * called upon incoming connections.
     * @param connection
     */
    public void onAccept(NIOConnection connection);

    /**
     * called when a connection was established
     * make sure to issue the first write
     * @param connection
     */
    public void onConnect(NIOConnection connection);

    /**
     * called when new data is available
     * @param connection
     * @param buffer
     */
    public void onRead(NIOConnection connection, ReusableBuffer buffer);

    /**
     * called when a connection is closed
     * @param connection
     */
    public void onClose(NIOConnection connection);


    public void onWriteFailed(IOException exception, Object context);

    public void onConnectFailed(InetSocketAddress endpoint, IOException exception, Object context);

}