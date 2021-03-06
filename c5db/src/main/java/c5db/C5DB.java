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

import c5db.discovery.BeaconService;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.server.ConfigKeyUpdated;
import c5db.interfaces.server.ModuleStateChange;
import c5db.log.LogFileService;
import c5db.log.LogService;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import c5db.messages.generated.StartModule;
import c5db.messages.generated.StopModule;
import c5db.regionserver.RegionServerService;
import c5db.replication.ReplicatorService;
import c5db.tablet.TabletService;
import c5db.util.C5FiberFactory;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberOnly;
import c5db.util.PoolFiberFactoryWithExecutor;
import c5db.webadmin.WebAdminService;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.protostuff.Message;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.core.Disposable;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.jetlang.fibers.ThreadFiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Holds information about all other modules, can start/stop other modules, etc.
 * Knows the 'root' information about this server as well, such as NodeId, etc.
 * <p/>
 * To shut down the 'server' module is to shut down the server.
 */
public class C5DB extends AbstractService implements C5Server {
  private static final Logger LOG = LoggerFactory.getLogger(C5DB.class);

  private final String clusterName;
  private final long nodeId;
  private final ConfigDirectory configDirectory;

  private final Channel<CommandRpcRequest<?>> commandChannel = new MemoryChannel<>();
  private final SettableFuture<Void> shutdownFuture = SettableFuture.create();

  private Fiber serverFiber;
  private PoolFiberFactory fiberPool;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;

  private final Map<ModuleType, C5Module> allModules = new HashMap<>();


  public C5DB(ConfigDirectory configDirectory) throws IOException {
    this.configDirectory = configDirectory;

    String data = configDirectory.getNodeId();
    long toNodeId = 0;
    if (data != null) {
      try {
        toNodeId = Long.parseLong(data);
      } catch (NumberFormatException ignored) {
        throw new RuntimeException("NodeId not set");
      }
    }

    if (toNodeId == 0) {
      throw new RuntimeException("NodeId not set");
    }

    this.nodeId = toNodeId;

    if (System.getProperties().containsKey(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME)) {
      this.clusterName = System.getProperty(C5ServerConstants.CLUSTER_NAME_PROPERTY_NAME);
    } else {
      this.clusterName = C5ServerConstants.LOCALHOST;
    }
  }

  @Override
  public long getNodeId() {
    return nodeId;
  }

  @Override
  public ListenableFuture<C5Module> getModule(final ModuleType moduleType) {
    final SettableFuture<C5Module> future = SettableFuture.create();
    serverFiber.execute(() -> {

      // What happens iff the allModules has EMPTY?
      if (!allModules.containsKey(moduleType)) {
        // listen to the registration stream:
        final Disposable[] d = new Disposable[]{null};
        d[0] = getModuleStateChangeChannel().subscribe(serverFiber, message -> {
          if (message.state != State.RUNNING) {
            return;
          }

          if (message.module.getModuleType().equals(moduleType)) {
            future.set(message.module);

            assert d[0] != null;  // this is pretty much impossible because of how fibers work.
            d[0].dispose();
          }
        });
      }

      future.set(allModules.get(moduleType));
    });
    return future;
  }

  @Override
  public ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException {
    final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
    serverFiber.execute(() -> {
      future.set(ImmutableMap.copyOf(allModules));
    });
    return future.get();
  }

  @Override
  public ListenableFuture<ImmutableMap<ModuleType, C5Module>> getModules2() {
    final SettableFuture<ImmutableMap<ModuleType, C5Module>> future = SettableFuture.create();
    serverFiber.execute(() -> {
      future.set(ImmutableMap.copyOf(allModules));
    });
    return future;
  }

  public RequestChannel<CommandRpcRequest<?>, CommandReply> commandRequests = new MemoryRequestChannel<>();

  @Override
  public RequestChannel<CommandRpcRequest<?>, CommandReply> getCommandRequests() {
    return commandRequests;
  }

  @Override
  public Channel<CommandRpcRequest<?>> getCommandChannel() {
    return commandChannel;
  }

  private final Channel<ModuleStateChange> serviceRegisteredChannel = new MemoryChannel<>();

  @Override
  public Channel<ModuleStateChange> getModuleStateChangeChannel() {
    return serviceRegisteredChannel;
  }

  @Override
  public ConfigDirectory getConfigDirectory() {
    return configDirectory;
  }

  @Override
  public boolean isSingleNodeMode() {
    return this.clusterName.equals(C5ServerConstants.LOCALHOST);
  }

  @Override
  public Channel<ConfigKeyUpdated> getConfigUpdateChannel() {
    // TODO this
    return null;
  }

  @Override
  public C5FiberFactory getFiberFactory(Consumer<Throwable> throwableConsumer) {
    return new PoolFiberFactoryWithExecutor(fiberPool,
        new ExceptionHandlingBatchExecutor(throwableConsumer));
  }

  @Override
  public ListenableFuture<Void> getShutdownFuture() {
    return shutdownFuture;
  }

  @Override
  protected void doStart() {
    try {
      // TODO this should be done as part of the log file service startup, if at all.
      new LogFileService(configDirectory.getBaseConfigPath()).clearOldArchivedLogs(0);
    } catch (IOException e) {
      notifyFailed(e);
    }

    try {
      serverFiber = new ThreadFiber(new RunnableExecutorImpl(), "C5-Server", false);
      fiberPool = new PoolFiberFactory(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup();

      commandChannel.subscribe(serverFiber, message -> {
        try {
          processCommandMessage(message);
        } catch (Exception e) {
          LOG.warn("exception during message processing", e);
        }
      });

      commandRequests.subscribe(serverFiber, this::processCommandRequest);

      serverFiber.start();

      notifyStarted();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    serverFiber.dispose();
    fiberPool.dispose();

    notifyStopped();
  }

  private void processCommandMessage(CommandRpcRequest<?> msg) throws Exception {
    processCommandSubMessage(msg.message);
  }

  @FiberOnly
  private void processCommandSubMessage(Message<?> msg) throws Exception {
    if (msg instanceof StartModule) {
      StartModule message = (StartModule) msg;
      startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());
    } else if (msg instanceof StopModule) {
      StopModule message = (StopModule) msg;
      stopModule(message.getModule(), message.getHardStop(), message.getStopReason());
    } else if (msg instanceof ModuleSubCommand) {
      processModuleSubCommand((ModuleSubCommand) msg);
    }
  }

  private void processModuleSubCommand(ModuleSubCommand msg) throws InterruptedException {
    if (msg.getModule().equals(ModuleType.Tablet)) {
      C5Module module = this.allModules.get(msg.getModule());
      String result = module.acceptCommand(msg.getSubCommand());
      LOG.debug("accept command: " + result);
    }
  }

  @FiberOnly
  private void processCommandRequest(Request<CommandRpcRequest<?>, CommandReply> request) {
    CommandRpcRequest<?> r = request.getRequest();
    Message<?> subMessage = r.message;
    long receiptNodeId = r.receipientNodeId;

    try {
      String stdout;

      if (subMessage instanceof StartModule) {
        StartModule message = (StartModule) subMessage;
        startModule(message.getModule(), message.getModulePort(), message.getModuleArgv());

        stdout = String.format("Module %s started", message.getModule());
      } else if (subMessage instanceof StopModule) {
        StopModule message = (StopModule) subMessage;

        stopModule(message.getModule(), message.getHardStop(), message.getStopReason());

        stdout = String.format("Module %s started", message.getModule());
      } else if (subMessage instanceof ModuleSubCommand) {
        // how this works:
        // - pluck out the module
        // - call the thingy
        // - collect the reply
        // reply.
        stdout = "";
        ModuleSubCommand moduleSubCommand = (ModuleSubCommand) subMessage;
        ModuleType moduleTypeToIssueCommandTo = moduleSubCommand.getModule();
        C5Module module = this.allModules.get(moduleTypeToIssueCommandTo);
        if (module == null) {
          stdout = "Module type " + moduleTypeToIssueCommandTo + " is not running!";
        } else {
          String result = module.acceptCommand(moduleSubCommand.getSubCommand());
          if (result == null) {
            stdout = "(null) - module doesn't support commands";
          } else {
            stdout = result;
          }
        }
      } else {
        CommandReply reply = new CommandReply(false,
            "",
            String.format("Unknown message type: %s", r.getClass()));

        request.reply(reply);
        return;
      }

      CommandReply reply = new CommandReply(true, stdout, "");
      request.reply(reply);

    } catch (Exception e) {
      CommandReply reply = new CommandReply(false, "", e.toString());
      request.reply(reply);
    }
  }

  @FiberOnly
  private boolean startModule(final ModuleType moduleType, final int modulePort, String moduleArgv) throws Exception {
    if (allModules.containsKey(moduleType)) {
      LOG.warn("Module {} already running", moduleType);
      throw new Exception("Cant start, running, module: " + moduleType);
    }

    switch (moduleType) {
      case Discovery: {
        Map<ModuleType, Integer> l = new HashMap<>();
        for (ModuleType name : allModules.keySet()) {
          l.put(name, allModules.get(name).port());
        }

        C5Module module = new BeaconService(this.nodeId, modulePort, fiberPool.create(), workerGroup, l, this);
        startServiceModule(module);
        break;
      }
      case Replication: {
        C5Module module = new ReplicatorService(bossGroup, workerGroup, modulePort, this);
        startServiceModule(module);
        break;
      }
      case Log: {
        C5Module module = new LogService(this);
        startServiceModule(module);

        break;
      }
      case Tablet: {
        C5Module module = new TabletService(this);
        startServiceModule(module);

        break;
      }
      case RegionServer: {
        C5Module module = new RegionServerService(bossGroup, workerGroup, modulePort, this);
        startServiceModule(module);

        break;
      }
      case WebAdmin: {
        C5Module module = new WebAdminService(this, modulePort);
        startServiceModule(module);

        break;
      }

      default:
        throw new Exception("No such module as " + moduleType);
    }

    return true;
  }

  private void startServiceModule(C5Module module) {
    LOG.info("Starting service {}", module.getModuleType());
    module.addListener(new ModuleStatePublisher(module), serverFiber);

    module.start();
    allModules.put(module.getModuleType(), module);
  }

  @FiberOnly
  private void stopModule(ModuleType moduleType, boolean hardStop, String stopReason) {
    Service theModule = allModules.get(moduleType);
    if (theModule == null) {
      LOG.debug("Cant stop module {}, not in registry", moduleType);
      return;
    }

    theModule.stop();
  }

  /**
   * Publishes state changes for the given module. It is up to the caller
   * to properly register an instance of this class and pass the SAME module
   * into the constructor (also on the server fiber too).
   */
  private class ModuleStatePublisher implements Listener {
    private final C5Module module;

    private ModuleStatePublisher(C5Module module) {
      this.module = module;
    }

    @Override
    public void starting() {
      LOG.debug("Starting module {}", module);
      publishEvent(State.STARTING);
    }

    @Override
    public void running() {
      LOG.debug("Running module {}", module);
      publishEvent(State.RUNNING);
    }

    @Override
    public void stopping(State from) {
      LOG.debug("Stopping module {}", module);
      publishEvent(State.STOPPING);
    }

    @Override
    public void terminated(State from) {
      LOG.debug("Terminated module {}", module);
      allModules.remove(module.getModuleType());
      publishEvent(State.TERMINATED);
    }

    @Override
    public void failed(State from, Throwable failure) {
      LOG.debug("Failed module " + module, failure);
      publishEvent(State.FAILED);
    }

    private void publishEvent(State state) {
      ModuleStateChange p = new ModuleStateChange(module, state);
      getModuleStateChangeChannel().publish(p);
    }
  }
}
