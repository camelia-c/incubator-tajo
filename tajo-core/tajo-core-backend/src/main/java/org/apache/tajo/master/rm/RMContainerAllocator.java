/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.master.rm;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.tajo.SubQueryId;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.querymaster.QueryMaster.QueryContext;
import org.apache.tajo.master.querymaster.SubQueryState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RMContainerAllocator extends AMRMClientImpl
    implements EventHandler<ContainerAllocationEvent> {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(RMContainerAllocator.
      class.getName());

  private QueryContext context;
  private final EventHandler eventHandler;

  public RMContainerAllocator(QueryContext context) {
    super(context.getApplicationAttemptId());
    this.context = context;
    this.eventHandler = context.getDispatcher().getEventHandler();
  }

  public void init(Configuration conf) {
    super.init(conf);
  }

  private static final int WAIT_INTERVAL_AVAILABLE_NODES = 500; // 0.5 second
  public void start() {
    super.start();

    RegisterApplicationMasterResponse response;
    try {
      response = registerApplicationMaster("locahost", 10080, "http://localhost:1234");
      context.setMaxContainerCapability(response.getMaximumResourceCapability().getMemory());
      context.setMinContainerCapability(response.getMinimumResourceCapability().getMemory());

      // If the number of cluster nodes is ZERO, it waits for available nodes.
      AllocateResponse allocateResponse = allocate(0.0f);
      while(allocateResponse.getNumClusterNodes() < 1) {
        try {
          Thread.sleep(WAIT_INTERVAL_AVAILABLE_NODES);
          LOG.info("Waiting for Available Cluster Nodes");
          allocateResponse = allocate(0);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
      context.setNumClusterNodes(allocateResponse.getNumClusterNodes());
    } catch (YarnRemoteException e) {
      LOG.error(e);
    }

    startAllocatorThread();
  }

  protected Thread allocatorThread;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private int rmPollInterval = 100;//millis
  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            try {
              heartbeat();
            } catch (YarnException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              // TODO: for other exceptions
            }
            Thread.sleep(rmPollInterval);
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            break;
          }
        }
        LOG.info("Allocated thread stopped");
      }
    });
    allocatorThread.setName("RMContainerAllocator");
    allocatorThread.start();
  }

  public void stop() {
    stopped.set(true);
    allocatorThread.interrupt();
    LOG.info("RMContainerAllocator stopped");
    super.stop();
  }

  private final Map<Priority, SubQueryId> subQueryMap =
      new HashMap<Priority, SubQueryId>();
  private AtomicLong prevReportTime = new AtomicLong(0);
  private int reportInterval = 5 * 1000; // second

  public void heartbeat() throws Exception {
    AllocateResponse allocateResponse = allocate(context.getProgress());
    AMResponse response = allocateResponse.getAMResponse();
    List<Container> allocatedContainers = response.getAllocatedContainers();

    long currentTime = System.currentTimeMillis();
    if((currentTime - prevReportTime.longValue()) >= reportInterval){
      LOG.debug("Available Cluster Nodes: " + allocateResponse.getNumClusterNodes());
      LOG.debug("Num of Allocated Containers: " + allocatedContainers.size());
      LOG.info("Available Resource: " + response.getAvailableResources());
      prevReportTime.set(currentTime);
    }

    if (allocatedContainers.size() > 0) {
      LOG.info("================================================================");
      for (Container container : response.getAllocatedContainers()) {
        LOG.info("> Container Id: " + container.getId());
        LOG.info("> Node Id: " + container.getNodeId());
        LOG.info("> Resource (Mem): " + container.getResource().getMemory());
        LOG.info("> State : " + container.getState());
        LOG.info("> Priority: " + container.getPriority());
      }
      LOG.info("================================================================");

      Map<SubQueryId, List<Container>> allocated = new HashMap<SubQueryId, List<Container>>();

      for (Container container : allocatedContainers) {
        SubQueryId subQueryId = subQueryMap.get(container.getPriority());
        SubQueryState state = context.getSubQuery(subQueryId).getState();
        if (!(isRunningState(state))) {
          releaseAssignedContainer(container.getId());
        } else {
          if (allocated.containsKey(subQueryId)) {
            allocated.get(subQueryId).add(container);
          } else {
            allocated.put(subQueryId, Lists.newArrayList(container));
          }
        }
      }

      for (Entry<SubQueryId, List<Container>> entry : allocated.entrySet()) {
        eventHandler.handle(new SubQueryContainerAllocationEvent(entry.getKey(), entry.getValue()));
      }
    }
  }

  private static boolean isRunningState(SubQueryState state) {
    return state == SubQueryState.INIT || state == SubQueryState.NEW ||
        state == SubQueryState.CONTAINER_ALLOCATED || state == SubQueryState.RUNNING;
  }

  @Override
  public void handle(ContainerAllocationEvent event) {

    if (event.getType() == ContainerAllocatorEventType.CONTAINER_REQ) {
      LOG.info(event);
      subQueryMap.put(event.getPriority(), event.getSubQueryId());
      addContainerRequest(new ContainerRequest(event.getCapability(), null, null,
          event.getPriority(), event.getRequiredNum()));

    } else if (event.getType() == ContainerAllocatorEventType.CONTAINER_DEALLOCATE) {
      LOG.info(event);
    } else {
      LOG.info(event);
    }
  }
}
