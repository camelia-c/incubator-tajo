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

package org.apache.tajo.master.querymaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.QueryMeta;
import org.apache.tajo.master.TajoAsyncDispatcher;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.master.rm.WorkerResourceManager;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.ProtoAsyncRpcClient;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryInProgress extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryInProgress.class.getName());

  private QueryId queryId;

  private QueryMeta queryMeta;

  private TajoAsyncDispatcher dispatcher;

  private LogicalRootNode plan;

  private AtomicBoolean querySubmitted = new AtomicBoolean(false);

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private QueryInfo queryInfo;

  private final TajoMaster.MasterContext masterContext;

  private ProtoAsyncRpcClient queryMasterRpc;

  private TajoWorkerProtocol.TajoWorkerProtocolService queryMasterRpcClient;

  public QueryInProgress(
      TajoMaster.MasterContext masterContext,
      QueryMeta queryMeta,
      QueryId queryId, String sql, LogicalRootNode plan) {
    super(QueryInProgress.class.getName());
    this.masterContext = masterContext;
    this.queryMeta = queryMeta;
    this.queryId = queryId;
    this.plan = plan;

    queryInfo = new QueryInfo(queryId, sql);
    queryInfo.setStartTime(System.currentTimeMillis());
  }

  @Override
  public void init(Configuration conf) {
    dispatcher = new TajoAsyncDispatcher("QueryInProgress:" + queryId);
    this.addService(dispatcher);

    dispatcher.register(QueryJobEvent.Type.class, new QueryInProgressEventHandler());
    super.init(conf);
  }

  @Override
  public void stop() {
    synchronized(stopped) {
      if(stopped.get()) {
        return;
      }
      stopped.set(true);
    }
    LOG.info("=========================================================");
    LOG.info("Stop query:" + queryId);

    masterContext.getResourceManager().stopQueryMaster(queryId);

    boolean queryMasterStopped = false;
    long startTime = System.currentTimeMillis();
    while(true) {
      try {
        if(masterContext.getResourceManager().isQueryMasterStopped(queryId)) {
          LOG.info("====> " + queryId + " QueryMaster stopped");
          queryMasterStopped = true;
          break;
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
      if(System.currentTimeMillis() - startTime > 60 * 1000) {
        LOG.warn("Failed to stop QueryMaster:" + queryId);
        break;
      }
    }

    if(queryMasterRpc != null) {
      //TODO release to connection pool
      queryMasterRpc.close();
    }
    super.stop();
  }

  @Override
  public void start() {
    super.start();
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public void startQueryMaster() {
    try {
      LOG.info("Initializing QueryInProgress for QueryID=" + queryId);
      WorkerResourceManager resourceManager = masterContext.getResourceManager();
      WorkerResource queryMasterResource = resourceManager.allocateQueryMaster(this);

      if(queryMasterResource != null) {
        queryInfo.setQueryMasterResource(queryMasterResource);
      }
      getEventHandler().handle(new QueryJobEvent(QueryJobEvent.Type.QUERY_MASTER_START, queryInfo));
    } catch (Exception e) {
      catchException(e);
    }
  }

  class QueryInProgressEventHandler implements EventHandler<QueryJobEvent> {
    @Override
    public void handle(QueryJobEvent queryJobEvent) {
      if(queryJobEvent.getType() == QueryJobEvent.Type.QUERY_JOB_HEARTBEAT) {
        heartbeat(queryJobEvent.getQueryInfo());
      } else if(queryJobEvent.getType() == QueryJobEvent.Type.QUERY_MASTER_START) {
        masterContext.getResourceManager().startQueryMaster(QueryInProgress.this);
      } else if(queryJobEvent.getType() == QueryJobEvent.Type.QUERY_JOB_START) {
        submmitQueryToMaster();
      } else if(queryJobEvent.getType() == QueryJobEvent.Type.QUERY_JOB_FINISH) {
        stop();
      }
    }
  }

  public TajoWorkerProtocol.TajoWorkerProtocolService getQueryMasterRpcClient() {
    return queryMasterRpcClient;
  }

  private void connectQueryMaster() throws Exception {
    if(queryInfo.getQueryMasterResource() != null &&
        queryInfo.getQueryMasterResource().getAllocatedHost() != null) {
      InetSocketAddress addr = NetUtils.createSocketAddr(
          queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort());
      LOG.info("Connect to QueryMaster:" + addr);
      //TODO Get Connection from pool
      queryMasterRpc = new ProtoAsyncRpcClient(TajoWorkerProtocol.class, addr);
      queryMasterRpcClient = queryMasterRpc.getStub();
    }
  }

  private synchronized void submmitQueryToMaster() {
    if(querySubmitted.get()) {
      return;
    }

    try {
      if(queryMasterRpcClient == null) {
        connectQueryMaster();
      }
      if(queryMasterRpcClient == null) {
        LOG.info("No QueryMaster conneciton info.");
        //TODO wait
        return;
      }
      LOG.info("Call executeQuery to :" +
          queryInfo.getQueryMasterHost() + ":" + queryInfo.getQueryMasterPort() + "," + queryId);
      queryMasterRpcClient.executeQuery(
          null,
          TajoWorkerProtocol.QueryExecutionRequestProto.newBuilder()
              .setQueryId(queryId.getProto())
              .setQueryMeta(queryMeta.getProto())
              .setLogicalPlanJson(PrimitiveProtos.StringProto.newBuilder().setValue(plan.toJson()).build())
              .build(), NullCallback.get());
      querySubmitted.set(true);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void catchException(Exception e) {
    LOG.error(e.getMessage(), e);
    queryInfo.setQueryState(TajoProtos.QueryState.QUERY_FAILED);
    queryInfo.setLastMessage(StringUtils.stringifyException(e));
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryInfo getQueryInfo() {
    return this.queryInfo;
  }

  private void heartbeat(QueryInfo queryInfo) {
    LOG.info("Received QueryMaster heartbeat:" + queryInfo);
    if(queryInfo.getQueryMasterResource() != null) {
      this.queryInfo.setQueryMasterResource(queryInfo.getQueryMasterResource());
    }
    this.queryInfo.setQueryState(queryInfo.getQueryState());

    if(queryInfo.getLastMessage() != null && !queryInfo.getLastMessage().isEmpty()) {
      this.queryInfo.setLastMessage(queryInfo.getLastMessage());
      LOG.info(queryId + queryInfo.getLastMessage());
    }
    if(this.queryInfo.getQueryState() == TajoProtos.QueryState.QUERY_FAILED) {
      //TODO needed QueryMaster's detail status(failed before or after launching worker)
      //queryMasterStopped.set(true);
      LOG.warn(queryId + " failed, " + queryInfo.getLastMessage());
    }

    if(!querySubmitted.get()) {
      getEventHandler().handle(
          new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_START, this.queryInfo));
    }

    if(isFinishState(this.queryInfo.getQueryState())) {
      getEventHandler().handle(
          new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_FINISH, this.queryInfo));
    }
  }

  private boolean isFinishState(TajoProtos.QueryState state) {
    return state == TajoProtos.QueryState.QUERY_FAILED ||
        state == TajoProtos.QueryState.QUERY_KILLED ||
        state == TajoProtos.QueryState.QUERY_SUCCEEDED;
  }
}
