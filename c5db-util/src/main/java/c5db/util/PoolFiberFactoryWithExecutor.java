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

package c5db.util;

import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;

import java.util.function.Consumer;

/**
 * Wrapper for a {@link org.jetlang.fibers.PoolFiberFactory} that creates fibers with a specific batch executor.
 * A single PoolFiberFactory instance may be wrapped by several instances of this class; so, this class doesn't
 * take responsibility for calling the PoolFiberFactory's dispose() method.
 */
public class PoolFiberFactoryWithExecutor implements C5FiberFactory {
  private final PoolFiberFactory fiberFactory;
  private final BatchExecutor batchExecutor;

  public PoolFiberFactoryWithExecutor(PoolFiberFactory fiberFactory, BatchExecutor batchExecutor) {
    this.fiberFactory = fiberFactory;
    this.batchExecutor = batchExecutor;
  }

  public static PoolFiberFactoryWithExecutor
  factoryWithExceptionHandler(PoolFiberFactory poolFiberFactory,
                              Consumer<Throwable> exceptionHandler) {
    return new PoolFiberFactoryWithExecutor(
        poolFiberFactory,
        new ExceptionHandlingBatchExecutor(exceptionHandler)
    );
  }

  /**
   * Create a new Fiber using the fiber factory and batch executor associated with this instance.
   *
   * @return Fiber instance
   */
  @Override
  public Fiber create() {
    // TODO this is a workaround until issue 252 is fixed.
    return new ThreadFiber(new RunnableExecutorImpl(batchExecutor), null, false);
    // return fiberFactory.create(batchExecutor);
  }
}
