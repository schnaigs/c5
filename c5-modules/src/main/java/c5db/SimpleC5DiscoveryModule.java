package c5db;

import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.discovery.NodeInfoReply;
import c5db.interfaces.discovery.NodeInfoRequest;
import c5db.messages.generated.ModuleType;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.RequestChannel;
import org.jetlang.channels.Subscriber;

import java.util.List;
import java.util.Map;

/**
 * This will be an implementation of DiscoveryModule that will have discovery "hard-coded,"
 * it will use a static map of nodeIds
 */
public class SimpleC5DiscoveryModule extends AbstractService implements DiscoveryModule {

  private final Map<Long, NodeInfo> nodeInfoMap;

  public SimpleC5DiscoveryModule(Map<Long, NodeInfo> nodeIDs) {
    this.nodeInfoMap = nodeIDs;
  }

  @Override
  protected void doStart() {

  }

  @Override
  protected void doStop() {

  }

  private final RequestChannel<NodeInfoRequest, NodeInfoReply> nodeInfoRequests = new MemoryRequestChannel<>();

  @Override
  public RequestChannel<NodeInfoRequest, NodeInfoReply> getNodeInfo() {
    return nodeInfoRequests;
  }

  @Override
  public ListenableFuture<NodeInfoReply> getNodeInfo(long nodeId, ModuleType module) {
    SettableFuture<NodeInfoReply> future = SettableFuture.create();
    NodeInfo peer = nodeInfoMap.get(nodeId);
    if (peer == null) {
      future.set(NodeInfoReply.NO_REPLY);
    } else {
      Integer servicePort = peer.modules.get(module);
      if (servicePort == null) {
        future.set(NodeInfoReply.NO_REPLY);
      } else {
        List<String> peerAddresses = peer.availability.getAddressesList();
        future.set(new NodeInfoReply(true, peerAddresses, servicePort));
      }
    }
    return future;
  }

  private ImmutableMap<Long, NodeInfo> getCopyOfState() {
    return ImmutableMap.copyOf(nodeInfoMap);
  }

  @Override
  public ListenableFuture<ImmutableMap<Long, NodeInfo>> getState() {
    final SettableFuture<ImmutableMap<Long, NodeInfo>> future = SettableFuture.create();
    future.set(this.getCopyOfState());
    return future;
  }

  private final org.jetlang.channels.Channel<NewNodeVisible> newNodeVisibleChannel = new MemoryChannel<>();

  @Override
  public Subscriber<NewNodeVisible> getNewNodeNotifications() {
    return newNodeVisibleChannel;
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.Discovery;
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
  public String acceptCommand(String commandString) throws InterruptedException {
    return null;
  }

}
