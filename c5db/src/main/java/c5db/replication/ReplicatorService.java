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

import c5db.codec.ProtostuffDecoder;
import c5db.codec.ProtostuffEncoder;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.LogModule;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.discovery.NodeInfoRequest;
import c5db.interfaces.replication.IndexCommitNotice;
import c5db.interfaces.replication.Replicator;
import c5db.interfaces.replication.ReplicatorInstanceEvent;
import c5db.log.Mooring;
import c5db.messages.generated.ModuleType;
import c5db.replication.generated.ReplicationWireMessage;
import c5db.replication.rpc.RpcReply;
import c5db.replication.rpc.RpcRequest;
import c5db.replication.rpc.RpcWireReply;
import c5db.replication.rpc.RpcWireRequest;
import c5db.util.FiberOnly;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.Request;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Session;
import org.jetlang.channels.SessionClosed;
import org.jetlang.core.Callback;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * An implementation of ReplicationModule using instances of ReplicatorInstance to handle each quorum.
 * <p/>
 * TODO we dont have a way to actually START a freaking ReplicatorInstance - YET.
 * TODO consider being symmetric in how we handle sent messages.
 */
public class ReplicatorService extends AbstractService implements ReplicationModule {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicatorService.class);

  /**
   * ************* C5Module informational methods ***********************************
   */

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Replication;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return this.port;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }

  private MemoryChannel<IndexCommitNotice> indexCommitNotices = new MemoryChannel<>();

  @Override
  public ListenableFuture<Replicator> createReplicator(final String quorumId,
                                                       final List<Long> peers) {
    final SettableFuture<Replicator> future = SettableFuture.create();
    fiber.execute(new Runnable() {
      @Override
      public void run() {
        if (replicatorInstances.containsKey(quorumId)) {
          LOG.debug("Replicator for quorum {} exists already", quorumId);
          future.set(replicatorInstances.get(quorumId));
          return;
        }

        // We need to make sure that it's not coming from another singlemode daemon.
        if (!peers.contains(server.getNodeId()) && !server.isSingleNodeMode()) {
          LOG.error("Creating replicator for {}, peer list didnt contain myself", quorumId, peers);
          peers.add(server.getNodeId());
        }
        LOG.info("Creating replicator instance for {} peers {}", quorumId, peers);

        Mooring logMooring;
        try {
          logMooring = logModule.getMooring(quorumId);
        } catch (IOException e) {
          LOG.error("Unable to start Mooring for {} peers {}", quorumId, peers);
          future.setException(e);
          return;
        }

        MemoryChannel<Throwable> throwableChannel = new MemoryChannel<>();
        Fiber instanceFiber = server.getFiberFactory(throwableChannel::publish).create();
        ReplicatorInstance instance =
            new ReplicatorInstance(
                instanceFiber,
                server.getNodeId(),
                quorumId,
                logMooring,
                new Info(),
                persister,
                outgoingRequests,
                replicatorStateChanges,
                indexCommitNotices
            );
        instance.bootstrapQuorum(peers);
        throwableChannel.subscribe(fiber, instance::failReplicatorInstance);
        replicatorInstances.put(quorumId, instance);
        future.set(instance);
      }
    });
    return future;
  }

  @Override
  public org.jetlang.channels.Channel<IndexCommitNotice> getIndexCommitNotices() {
    return indexCommitNotices;
  }

  private MemoryChannel<ReplicatorInstanceEvent> replicatorStateChanges = new MemoryChannel<>();

  @Override
  public org.jetlang.channels.Channel<ReplicatorInstanceEvent> getReplicatorEventChannel() {
    return replicatorStateChanges;
  }

  // TODO this should be actually via whatever configuration system we end up using.
  private static class Info implements ReplicatorInformationInterface {

    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }

    @Override
    public long electionCheckRate() {
      return 100;
    }

    @Override
    public long electionTimeout() {
      return 1000;
    }

    @Override
    public long groupCommitDelay() {
      return 100;
    }
  }

  private final int port;
  private final C5Server server;
  private final Fiber fiber;
  private final NioEventLoopGroup bossGroup;
  private final NioEventLoopGroup workerGroup;

  private final Map<Long, Channel> connections = new HashMap<>();
  private final Map<String, ReplicatorInstance> replicatorInstances = new HashMap<>();
  private final ChannelGroup allChannels;
  // map of message_id -> Request
  // TODO we need a way to remove these after a while, because if we fail to get a reply we will be unhappy.
  private final Map<Long, Request<RpcRequest, RpcWireReply>> outstandingRPCs = new HashMap<>();
  private final Map<Session, Long> outstandingRPCbySession = new HashMap<>();

  private final RequestChannel<RpcRequest, RpcWireReply> outgoingRequests = new MemoryRequestChannel<>();
  private final Persister persister;

  private ServerBootstrap serverBootstrap;
  private Bootstrap outgoingBootstrap;

  // Initalized in the module start, by the time any messages or fiber executions trigger, this should be not-null
  private DiscoveryModule discoveryModule = null;
  private LogModule logModule = null;
  private Channel listenChannel;

  private long messageIdGen = 1;

  public ReplicatorService(NioEventLoopGroup bossGroup,
                           NioEventLoopGroup workerGroup,
                           int port, C5Server server) {
    this.bossGroup = bossGroup;
    this.workerGroup = workerGroup;
    this.port = port;
    this.server = server;
    this.fiber = server.getFiberFactory(this::failModule).create();
    this.allChannels = new DefaultChannelGroup(workerGroup.next());

    this.persister = new Persister(server.getConfigDirectory());
  }

  /**
   * *************** Handlers for netty/messages from the wire/TCP ***********************
   */
  @ChannelHandler.Sharable
  private class MessageHandler extends SimpleChannelInboundHandler<ReplicationWireMessage> {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      allChannels.add(ctx.channel());

      super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ReplicationWireMessage msg) throws Exception {
      fiber.execute(() -> {
        handleWireInboundMessage(ctx.channel(), msg);
      });
    }
  }

  @FiberOnly
  private void handleWireInboundMessage(Channel channel, ReplicationWireMessage msg) {
    long messageId = msg.getMessageId();
    if (msg.getReceiverId() != this.server.getNodeId()) {
      LOG.debug("Got messageId {} for {} but I am {}, ignoring!", messageId, msg.getReceiverId(), server.getNodeId());
      return;
    }

    if (msg.getInReply()) {
      Request<RpcRequest, RpcWireReply> request = outstandingRPCs.get(messageId);
      if (request == null) {
        LOG.debug("Got a reply message_id {} which we don't track", messageId);
        return;
      }

      outstandingRPCs.remove(messageId);
      outstandingRPCbySession.remove(request.getSession());
      request.reply(new RpcWireReply(msg));
    } else {
      handleWireRequestMessage(channel, msg);
    }
  }

  @FiberOnly
  private void handleWireRequestMessage(final Channel channel, final ReplicationWireMessage msg) {
    RpcWireRequest wireRequest = new RpcWireRequest(msg);
    String quorumId = wireRequest.quorumId;

    ReplicatorInstance replInst = replicatorInstances.get(quorumId);
    if (replInst == null) {
      LOG.trace("Instance not found {} for message id {} from {} (normal during region bootstrap)",
          quorumId,
          msg.getMessageId(),
          msg.getSenderId());
      // TODO send RPC failure to the sender?
      return;
    }

    AsyncRequest.withOneReply(fiber, replInst.getIncomingChannel(), wireRequest, new Callback<RpcReply>() {
      @Override
      public void onMessage(RpcReply reply) {
        if (!channel.isOpen()) {
          // TODO cant signal comms failure, so just drop on the floor. Is there a better thing to do?
          return;
        }

        ReplicationWireMessage b = reply.getWireMessage(
            msg.getMessageId(),
            server.getNodeId(),
            msg.getSenderId(),
            true
        );
//                ReplicationWireMessage.Builder b = reply.getWireMessage();
//                b.setSenderId(server.getNodeId())
//                        .setInReply(true)
//                        .setQuorumId(msg.getQuorumId())
//                        .setReceiverId(msg.getSenderId())
//                        .setMessageId(msg.getMessageId());

        channel.writeAndFlush(b);
      }
    });
  }

  /**
   * ************* Handlers for Request<> from replicator instances ***********************************
   */
  @FiberOnly
  private void handleCancelledSession(Session session) {
    Long messageId = outstandingRPCbySession.get(session);
    outstandingRPCbySession.remove(session);
    if (messageId == null) {
      return;
    }
    LOG.trace("Removing cancelled RPC, message ID {}", messageId);
    outstandingRPCs.remove(messageId);
  }

  @FiberOnly
  private void handleOutgoingMessage(final Request<RpcRequest, RpcWireReply> message) {
    final RpcRequest request = message.getRequest();
    final long to = request.to;

    if (to == server.getNodeId()) {
      handleLoopBackMessage(message);
      return;
    }

    // check to see if we have a connection:
    Channel channel = connections.get(to);
    if (channel != null && channel.isOpen()) {
      sendMessage0(message, channel);
      return;
    } else if (channel != null) {
      // stale?
      LOG.debug("Removing stale !isOpen channel from connections.get() for peer {}", to);
      connections.remove(to);
    }

    NodeInfoRequest nodeInfoRequest = new NodeInfoRequest(to, ModuleType.Replication);
    AsyncRequest.withOneReply(fiber, discoveryModule.getNodeInfo(), nodeInfoRequest, new Callback<NodeInfoReply>() {
      @FiberOnly
      @Override
      public void onMessage(NodeInfoReply nodeInfoReply) {
        if (!nodeInfoReply.found) {
          LOG.debug("Can't find the info for the peer {}", to);
          // TODO signal TCP/transport layer failure in a better way
          //message.reply(null);
          return;
        }

        // what if existing outgoing connection attempt?
        Channel channel = connections.get(to);
        if (channel != null && channel.isOpen()) {
          sendMessage0(message, channel);
          return;
        } else if (channel != null) {
          LOG.debug("Removing stale2 !isOpen channel from connections.get() for peer {}", to);
          connections.remove(to);
        }

        // ok so we connect now:
        ChannelFuture channelFuture = outgoingBootstrap.connect(nodeInfoReply.addresses.get(0), nodeInfoReply.port);
        LOG.trace("Connecting to peer {} at address {} port {}", to, nodeInfoReply.addresses.get(0), nodeInfoReply.port);

        // the channel might not be open, so defer the write.
        connections.put(to, channelFuture.channel());
        channelFuture.channel().closeFuture().addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            fiber.execute(() -> {
              // remove only THIS channel. It might have been removed prior so.
              //LOG.debug("Close future fired for {} so we are removing it as a connection", to);
              connections.remove(to, future.channel());
            });
          }
        });

        // funny hack, if the channel future is already open, we execute immediately!
        channelFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              //LOG.debug("Connected to peer {}, sending message!", to);
              sendMessage0(message, future.channel());
            }
          }
        });
      }
    });
  }

  private void sendMessage0(final Request<RpcRequest, RpcWireReply> message, final Channel channel) {
    fiber.execute(new Runnable() {
      @FiberOnly
      @Override
      public void run() {

        RpcRequest request = message.getRequest();
        long to = request.to;
        long messageId = messageIdGen++;

        outstandingRPCs.put(messageId, message);
        outstandingRPCbySession.put(message.getSession(), messageId);

        LOG.trace("Sending message id {} to {} / {}", messageId, to, request.quorumId);

        ReplicationWireMessage wireMessage = request.getWireMessage(
            messageId,
            server.getNodeId(),
            to,
            false
        );

        channel.writeAndFlush(wireMessage);
      }
    });
  }

  private void handleLoopBackMessage(final Request<RpcRequest, RpcWireReply> origMessage) {
    final long toFrom = server.getNodeId(); // I am me.
    final RpcRequest request = origMessage.getRequest();
    final String quorumId = request.quorumId;

    // Funny thing we don't have a direct handle on who sent us this message, so we have to do this. Sok though.
    final ReplicatorInstance repl = replicatorInstances.get(quorumId);
    if (repl == null) {
      // rare failure condition, whereby the replicator died AFTER it send messages.
      return; // ignore the message.
    }

    final RpcWireRequest newRequest = new RpcWireRequest(toFrom, quorumId, request.message);
    AsyncRequest.withOneReply(fiber, repl.getIncomingChannel(), newRequest, new Callback<RpcReply>() {
      @Override
      public void onMessage(RpcReply msg) {
        assert msg.message != null;
        RpcWireReply newReply = new RpcWireReply(toFrom, quorumId, msg.message);
        origMessage.reply(newReply);
      }
    });
  }


  /**
   * ********** Service startup/registration and shutdown/termination **************
   */
  @Override
  protected void doStart() {
    // must start the fiber up early.
    fiber.start();


    fiber.execute(new Runnable() {
      @Override
      public void run() {
        // TODO this is a shitty way to do this, more refactoring is necessary.
        LOG.warn("ReplicatorService now waiting for module dependency on Log & BeaconService");

        ListenableFuture<C5Module> logListen = server.getModule(ModuleType.Log);
        try {
          logModule = (LogModule) logListen.get();
        } catch (InterruptedException | ExecutionException e) {
          notifyFailed(e);
        }

        ListenableFuture<C5Module> f = server.getModule(ModuleType.Discovery);
        Futures.addCallback(f, new FutureCallback<C5Module>() {
          @Override
          public void onSuccess(C5Module result) {
            discoveryModule = (DiscoveryModule) result;

            // finish init:
            try {
              ChannelInitializer<SocketChannel> initer = new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline p = ch.pipeline();
                  p.addLast("frameDecode", new ProtobufVarint32FrameDecoder());
                  p.addLast("pbufDecode", new ProtostuffDecoder<>(ReplicationWireMessage.getSchema()));

                  p.addLast("frameEncode", new ProtobufVarint32LengthFieldPrepender());
                  p.addLast("pbufEncoder", new ProtostuffEncoder<ReplicationWireMessage>());

                  p.addLast(new MessageHandler());
                }
              };

              serverBootstrap = new ServerBootstrap();
              serverBootstrap.group(bossGroup, workerGroup)
                  .channel(NioServerSocketChannel.class)
                  .option(ChannelOption.SO_REUSEADDR, true)
                  .option(ChannelOption.SO_BACKLOG, 100)
                  .childOption(ChannelOption.TCP_NODELAY, true)
                  .childHandler(initer);
              serverBootstrap.bind(port).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  if (future.isSuccess()) {
                    listenChannel = future.channel();
                  } else {
                    LOG.error("Unable to bind! ", future.cause());
                  }
                }
              });

              outgoingBootstrap = new Bootstrap();
              outgoingBootstrap.group(workerGroup)
                  .channel(NioSocketChannel.class)
                  .option(ChannelOption.SO_REUSEADDR, true)
                  .option(ChannelOption.TCP_NODELAY, true)
                  .handler(initer);

              outgoingRequests.subscribe(fiber, new Callback<Request<RpcRequest, RpcWireReply>>() {
                    @Override
                    public void onMessage(Request<RpcRequest, RpcWireReply> message) {
                      handleOutgoingMessage(message);
                    }
                  }, new Callback<SessionClosed<RpcRequest>>() {
                    @FiberOnly
                    @Override
                    public void onMessage(SessionClosed<RpcRequest> message) {
                      // Clean up cancelled requests.
                      handleCancelledSession(message.getSession());
                    }
                  }
              );

              replicatorStateChanges.subscribe(fiber, new Callback<ReplicatorInstanceEvent>() {
                @Override
                public void onMessage(ReplicatorInstanceEvent message) {
                  if (message.eventType == ReplicatorInstanceEvent.EventType.QUORUM_FAILURE) {
                    LOG.error("replicator {} indicates failure, removing. Error {}", message.instance,
                        message.error);
                    replicatorInstances.remove(message.instance.getQuorumId());
                  } else {
                    LOG.info("replicator indicates state change {}", message);
                  }
                }
              });

              notifyStarted();
            } catch (Exception e) {
              notifyFailed(e);
            }
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.error("ReplicatorService unable to retrieve BeaconService!", t);
            notifyFailed(t);
          }
        }, fiber);
      }
    });
  }

  protected void failModule(Throwable t) {
    LOG.error("ReplicatorService failure, shutting down all ReplicatorInstances");
    try {
      replicatorInstances.values().forEach(ReplicatorInstance::dispose);
      fiber.dispose();
      if (listenChannel != null) {
        listenChannel.close();
      }
      allChannels.close();
    } finally {
      notifyFailed(t);
    }
  }

  @Override
  protected void doStop() {
    fiber.execute(new Runnable() {
      @Override
      public void run() {
        final AtomicInteger countDown = new AtomicInteger(1);
        GenericFutureListener<? extends Future<? super Void>> listener = future -> {
          if (countDown.decrementAndGet() == 0) {
            notifyStopped();
          }

        };
        if (listenChannel != null) {
          countDown.incrementAndGet();
          listenChannel.close().addListener(listener);
        }

        allChannels.close().addListener(listener);
      }
    });
  }
}
