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

import c5db.interfaces.ReplicatorLog;
import c5db.messages.generated.ModuleType;

import java.io.IOException;

/**
 * The log module is responsible for running all the threads and IO for write-ahead-logging.
 * <p/>
 * The write-ahead-log is responsible for maintaining persistence in the face of node or machine
 * failure.
 */
@ModuleTypeBinding(ModuleType.Log)
public interface LogModule extends C5Module {
  public ReplicatorLog getMooring(String quorumId) throws IOException;
}
