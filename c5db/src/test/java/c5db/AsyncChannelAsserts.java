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

package c5db;

import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberOnly;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.jetlang.channels.Subscriber;
import org.jetlang.core.BatchExecutor;
import org.jetlang.core.RunnableExecutor;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.runners.model.MultipleFailureException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Helpers that allow us to assert or wait for channel messages from jetlang.
 * <p>
 * TODO currently we create a fiber thread for every instance we run, maybe
 * consider using a fiber pool.
 */
public class AsyncChannelAsserts {

  public static class ChannelListener<T> {
    final Fiber subscribedFiber;
    final ArrayBlockingQueue<T> messages;
    final List<Throwable> throwables;

    public ChannelListener(Fiber subscribedFiber, ArrayBlockingQueue<T> messages,
                           List<Throwable> throwables) {
      this.subscribedFiber = subscribedFiber;
      this.messages = messages;
      this.throwables = throwables;
    }

    public void dispose() {
      subscribedFiber.dispose();
    }
  }

  public static <T> ChannelListener<T> listenTo(Subscriber<T> channel) {
    List<Throwable> throwables = new ArrayList<>();
    BatchExecutor exceptionHandlingBatchExecutor = new ExceptionHandlingBatchExecutor(throwables::add);
    RunnableExecutor runnableExecutor = new RunnableExecutorImpl(exceptionHandlingBatchExecutor);
    Fiber channelSubscriberFiber = new ThreadFiber(runnableExecutor, null, true);
    ArrayBlockingQueue<T> messages = new ArrayBlockingQueue<>(1);
    channel.subscribe(channelSubscriberFiber, m -> {
      try {
        messages.put(m);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    channelSubscriberFiber.start();
    return new ChannelListener<>(channelSubscriberFiber, messages, throwables);
  }

  public static <T> Matcher<T> publishesMessage(Matcher<T> m) {
    return m;
  }

  /**
   * Waits for a message that matches the matcher, if it doesn't happen within a reasonable
   * and short time frame, it will throw an assertion failure.
   *
   * @param matcher the matcher which might match a message
   * @param <T>     type
   * @throws Throwable
   */
  public static <T> void assertEventually(ChannelListener<T> listener,
                                          Matcher<? super T> matcher) throws Throwable {
    helper(listener, matcher, true);
  }

  /**
   * Waits for a message that matches the matcher, if it doesn't happen within a reasonable
   * and short time frame, this method just returns.  No failures are thrown, the
   * assumption is mock expectations will illuminate the error.
   *
   * @param listener the listener you want
   * @param matcher  the matcher which might match a message
   * @param <T>      type
   * @throws Throwable
   */
  public static <T> void waitUntil(ChannelListener<T> listener,
                                   Matcher<? super T> matcher) throws Throwable {
    helper(listener, matcher, false);
  }

  private static <T> void helper(ChannelListener<T> listener,
                                 Matcher<? super T> matcher,
                                 boolean assertFail) throws Throwable {

    List<T> received = new ArrayList<>();
    while (true) {
      T msg = listener.messages.poll(5, TimeUnit.SECONDS);

      if (msg == null) {
        Description d = new StringDescription();
        matcher.describeTo(d);

        if (!received.isEmpty()) {
          d.appendText("we received messages:");
        }

        for (T m : received) {
          matcher.describeMismatch(m, d);
        }

        if (assertFail) {
          listener.throwables.add(new AssertionError("Failing waiting for " + d.toString()));
          MultipleFailureException.assertEmpty(listener.throwables);
        }

        return;
      }

      if (matcher.matches(msg)) {
        if (!listener.throwables.isEmpty()) {
          MultipleFailureException.assertEmpty(listener.throwables);
        }

        return;
      }

      received.add(msg);
    }
  }

  /**
   * Keeps track of all objects that have ever been produced by a channel (or any Subscriber) and provides
   * the capability to wait until a future object matches an arbitrary Matcher; or to return from the wait
   * immediately if any object already produced matches.
   *
   * @param <T> Type of channel object
   */
  public static class ChannelHistoryMonitor<T> {
    private final List<T> messageLog = Collections.<T>synchronizedList(new ArrayList<>());
    private final Map<Matcher<? super T>, SettableFuture<T>> waitingToMatch = new HashMap<>();
    private final Fiber fiber;
    private static final int WAIT_TIMEOUT = 5; // seconds

    public ChannelHistoryMonitor(Subscriber<T> subscriber, Fiber fiber) {
      this.fiber = fiber;
      subscriber.subscribe(fiber, this::onMessage);
    }

    @FiberOnly
    private void onMessage(T message) {
      messageLog.add(message);
      Iterator<Matcher<? super T>> it = waitingToMatch.keySet().iterator();
      while (it.hasNext()) {
        Matcher<? super T> matcher = it.next();
        if (matcher.matches(message)) {
          waitingToMatch.get(matcher).set(message);
          it.remove();
        }
      }
    }

    public boolean hasAny(Matcher<? super T> matcher) {
      synchronized (messageLog) {
        for (T element : messageLog) {
          if (matcher.matches(element)) {
            return true;
          }
        }
      }
      return false;
    }

    public T getLatest(Matcher<? super T> matcher) {
      synchronized (messageLog) {
        for (T element : Lists.reverse(messageLog)) {
          if (matcher.matches(element)) {
            return element;
          }
        }
      }
      return null;
    }

    public T waitFor(Matcher<? super T> matcher) {
      ListenableFuture<T> finished = future(matcher);

      try {
        return finished.get(WAIT_TIMEOUT, TimeUnit.SECONDS);
      } catch (Exception e) {
        Description d = new StringDescription();
        matcher.describeTo(d);
        throw new AssertionError("Failed waiting for " + d.toString(), e);
      }
    }

    public void forgetHistory() {
      messageLog.clear();
    }

    private ListenableFuture<T> future(Matcher<? super T> matcher) {
      SettableFuture<T> finished = SettableFuture.create();

      fiber.execute(() -> {
        synchronized (messageLog) {
          for (T element : messageLog) {
            if (matcher.matches(element)) {
              finished.set(element);
              return;
            }
          }
        }
        waitingToMatch.put(matcher, finished);
      });

      return finished;
    }
  }
}
