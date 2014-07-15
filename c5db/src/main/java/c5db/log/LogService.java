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

import c5db.ConfigDirectory;
import c5db.interfaces.LogModule;
import c5db.interfaces.replication.ReplicatorLog;
import c5db.messages.generated.ModuleType;
import c5db.util.KeySerializingExecutor;
import c5db.util.WrappingKeySerializingExecutor;
import com.google.common.util.concurrent.AbstractService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * The Log module.
 */
public class LogService extends AbstractService implements LogModule {
  public static final int WAL_THREAD_POOL_SIZE = 1;
  private final ConfigDirectory configDirectory;
  private OLog oLog;
  private final Map<String, Mooring> moorings = new HashMap<>();

  public LogService(ConfigDirectory configDirectory) {
    this.configDirectory = configDirectory;
  }

  @Override
  protected void doStart() {
    try {
      LogFileService logFileService = new LogFileService(configDirectory.getBaseConfigPath());
      KeySerializingExecutor executor = new WrappingKeySerializingExecutor(
          Executors.newFixedThreadPool(WAL_THREAD_POOL_SIZE));
      this.oLog = new QuorumDelegatingLog(
          logFileService,
          executor,
          NavigableMapOLogEntryOracle::new,
          InMemoryPersistenceNavigator::new);

      // TODO start the flush threads as necessary
      // TODO log maintenance threads can go here too.
      notifyStarted();
    } catch (IOException e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    try {
      oLog.close();
      oLog = null;
    } catch (IOException ignored) {
    } finally {
      notifyStopped();
    }
  }

  @Override
  public ReplicatorLog getMooring(String quorumId) throws IOException {
    // TODO change this to use futures and fibers to manage the concurrency?
    synchronized (moorings) {
      if (moorings.containsKey(quorumId)) {
        return moorings.get(quorumId);
      }
      Mooring m = new Mooring(oLog, quorumId);
      moorings.put(quorumId, m);
      return m;
    }
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Log;
  }

  @Override
  public boolean hasPort() {
    return false;
  }

  @Override
  public int port() {
    return 0;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }

  public class FlushThread implements Runnable {
    int i = 0;

    private void flushAndCompact(int i) throws IOException {
//        for (HRegion region : OnlineRegions.INSTANCE.regions()) {
//          region.flushcache();
//          region.getLog().rollWriter();
//          if (i % C5Constants.AMOUNT_OF_FLUSH_PER_COMPACT == 0) {
//            region.compactStores();
//          }
//          if (i % C5Constants.AMOUNT_OF_FLUSH_PER_OLD_LOG_CLEAR == 0) {
//            olog.clearOldLogs(System.currentTimeMillis()
//                    - C5Constants.OLD_LOG_CLEAR_AGE);
//          }
//        }
    }

    @Override
    public void run() {
      try {
        flushAndCompact(i++);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
