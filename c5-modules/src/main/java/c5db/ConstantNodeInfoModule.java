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
public class ConstantNodeInfoModule extends AbstractService implements DiscoveryModule {

  private final Map<Long, NodeInfo> nodeInfoMap; // where/how does this map get created
  // also should it be "immutable?" maybe? probably?
  // also where do "fibers" go
  // A: apparently I don't need to use fibers. makes sense, not much computation going on here. sweet.
  // also....tests

  public ConstantNodeInfoModule(Map<Long, NodeInfo> nodeInfoMap1) {
    this.nodeInfoMap = nodeInfoMap1;
  }

  @Override
  protected void doStart() {
    // wtf goes here
  }

  @Override
  protected void doStop() {
    // wtf goes HERE
  }

  private final RequestChannel<NodeInfoRequest, NodeInfoReply> nodeInfoRequests = new MemoryRequestChannel<>();

  @Override
  public RequestChannel<NodeInfoRequest, NodeInfoReply> getNodeInfo() {
    return nodeInfoRequests;
  }

  @Override
  public ListenableFuture<NodeInfoReply> getNodeInfo(long nodeId, ModuleType module) {
    // copied and simplified this from BeaconService -- is it still correct? workable?
    // it seems to make sense and it compiles, so...
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
