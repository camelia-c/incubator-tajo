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

package org.apache.tajo.master;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.function.Country;
import org.apache.tajo.engine.function.InCountry;
import org.apache.tajo.engine.function.builtin.*;
import org.apache.tajo.master.querymaster.QueryJobManager;
import org.apache.tajo.master.rm.TajoWorkerResourceManager;
import org.apache.tajo.master.rm.WorkerResourceManager;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.webapp.StaticHttpServer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class TajoMaster extends CompositeService {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TajoMaster.class);

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private MasterContext context;
  private TajoConf conf;
  private FileSystem defaultFS;
  private Clock clock;

  private Path basePath;
  private Path wareHousePath;

  private CatalogServer catalogServer;
  private CatalogService catalog;
  private StorageManager storeManager;
  private GlobalEngine globalEngine;
  private AsyncDispatcher dispatcher;
  private TajoMasterClientService tajoMasterClientService;
  private TajoMasterService tajoMasterService;

  private WorkerResourceManager resourceManager;
  //Web Server
  private StaticHttpServer webServer;

  private QueryJobManager queryJobManager;

  public TajoMaster() throws Exception {
    super(TajoMaster.class.getName());
  }

  @Override
  public void init(Configuration _conf) {
    this.conf = (TajoConf) _conf;

    context = new MasterContext(conf);
    clock = new SystemClock();

    try {
      RackResolver.init(conf);

//      this.conf.writeXml(System.out);
      String className = this.conf.get("tajo.resource.manager", TajoWorkerResourceManager.class.getCanonicalName());
      Class<WorkerResourceManager> resourceManagerClass =
          (Class<WorkerResourceManager>)Class.forName(className);

      Constructor<WorkerResourceManager> constructor = resourceManagerClass.getConstructor(MasterContext.class);
      resourceManager = constructor.newInstance(context);
      resourceManager.init(context.getConf());

      //TODO WebServer port configurable
      webServer = StaticHttpServer.getInstance(this ,"admin", null, 8080 ,
          true, null, context.getConf(), null);
      webServer.start();

      // Get the tajo base dir
      this.basePath = new Path(conf.getVar(ConfVars.ROOT_DIR));
      LOG.info("Tajo Root dir is set " + basePath);
      // Get default DFS uri from the base dir
      this.defaultFS = basePath.getFileSystem(conf);
      conf.set("fs.defaultFS", defaultFS.getUri().toString());
      LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");
      if (!defaultFS.exists(basePath)) {
        defaultFS.mkdirs(basePath);
        LOG.info("Tajo Base dir (" + basePath + ") is created.");
      }

      this.storeManager = new StorageManager(conf);

      // Get the tajo data warehouse dir
      this.wareHousePath = new Path(basePath, TajoConstants.WAREHOUSE_DIR);
      LOG.info("Tajo Warehouse dir is set to " + wareHousePath);
      if (!defaultFS.exists(wareHousePath)) {
        defaultFS.mkdirs(wareHousePath);
        LOG.info("Warehouse dir (" + wareHousePath + ") is created");
      }

      this.dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);

      // The below is some mode-dependent codes
      // If tajo is local mode
      final boolean mode = conf.getBoolVar(ConfVars.CLUSTER_DISTRIBUTED);
      if (!mode) {
        LOG.info("Enabled Pseudo Distributed Mode");
      } else { // if tajo is distributed mode
        LOG.info("Enabled Distributed Mode");
      }
      // This is temporal solution of the above problem.
      catalogServer = new CatalogServer(initBuiltinFunctions());
      addIfService(catalogServer);
      catalog = new LocalCatalog(catalogServer);

      globalEngine = new GlobalEngine(context);
      addIfService(globalEngine);

      queryJobManager = new QueryJobManager(context);
      addIfService(queryJobManager);

      tajoMasterClientService = new TajoMasterClientService(context);
      addIfService(tajoMasterClientService);

      tajoMasterService = new TajoMasterService(context);
      addIfService(tajoMasterService);
    } catch (Exception e) {
       LOG.error(e.getMessage(), e);
    }

    LOG.info("====> Tajo master started");
    super.init(conf);
  }

  @SuppressWarnings("unchecked")
  public static List<FunctionDesc> initBuiltinFunctions() throws ServiceException {
    List<FunctionDesc> sqlFuncs = new ArrayList<FunctionDesc>();

    // Sum
    sqlFuncs.add(new FunctionDesc("sum", SumInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("sum", SumLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("sum", SumFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("sum", SumDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8)));

    // Max
    sqlFuncs.add(new FunctionDesc("max", MaxInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("max", MaxLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("max", MaxFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("max", MaxDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8)));

    // Min
    sqlFuncs.add(new FunctionDesc("min", MinInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("min", MinLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("min", MinFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4 )));
    sqlFuncs.add(new FunctionDesc("min", MinDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8)));
    sqlFuncs.add(new FunctionDesc("min", MinString.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT),
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT)));

    // AVG
    sqlFuncs.add(new FunctionDesc("avg", AvgInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4)));
    sqlFuncs.add(new FunctionDesc("avg", AvgLong.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8),
        CatalogUtil.newDataTypesWithoutLen(Type.INT8)));
    sqlFuncs.add(new FunctionDesc("avg", AvgFloat.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT4)));
    sqlFuncs.add(new FunctionDesc("avg", AvgDouble.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8),
        CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8)));

    // Count
    sqlFuncs.add(new FunctionDesc("count", CountValue.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen(Type.ANY)));
    sqlFuncs.add(new FunctionDesc("count", CountRows.class, FunctionType.AGGREGATION,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen()));

    // GeoIP
    sqlFuncs.add(new FunctionDesc("in_country", InCountry.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.BOOLEAN),
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT, Type.TEXT)));
    sqlFuncs.add(new FunctionDesc("country", Country.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT),
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT)));

    // Date
    sqlFuncs.add(new FunctionDesc("date", Date.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen(Type.TEXT)));

    // Today
    sqlFuncs.add(new FunctionDesc("today", Date.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT8),
        CatalogUtil.newDataTypesWithoutLen()));

    sqlFuncs.add(
        new FunctionDesc("random", RandomInt.class, FunctionType.GENERAL,
            CatalogUtil.newDataTypesWithoutLen(Type.INT4),
            CatalogUtil.newDataTypesWithoutLen(Type.INT4)));

    return sqlFuncs;
  }

  public MasterContext getContext() {
    return this.context;
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  @Override
  public void start() {
    LOG.info("TajoMaster startup");
    super.start();
  }

  @Override
  public void stop() {
    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error(e);
    }

    super.stop();
    LOG.info("TajoMaster main thread exiting");
  }

  public EventHandler getEventHandler() {
    return dispatcher.getEventHandler();
  }

  public String getMasterServerName() {
    return null;
  }

  public boolean isMasterRunning() {
    return getServiceState() == STATE.STARTED;
  }

  public CatalogService getCatalog() {
    return this.catalog;
  }

  public StorageManager getStorageManager() {
    return this.storeManager;
  }

  public class MasterContext {
    private final TajoConf conf;

    public MasterContext(TajoConf conf) {
      this.conf = conf;
    }

    public TajoConf getConf() {
      return conf;
    }

    public Clock getClock() {
      return clock;
    }

    public QueryJobManager getQueryJobManager() {
      return queryJobManager;
    }

    public WorkerResourceManager getResourceManager() {
      return resourceManager;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public CatalogService getCatalog() {
      return catalog;
    }

    public GlobalEngine getGlobalEngine() {
      return globalEngine;
    }

    public StorageManager getStorageManager() {
      return storeManager;
    }

    public TajoMasterService getTajoMasterService() {
      return tajoMasterService;
    }
  }

  public static void main(String[] args) throws Exception {
    StringUtils.startupShutdownMessage(TajoMaster.class, args, LOG);

    try {
      TajoMaster master = new TajoMaster();
      ShutdownHookManager.get().addShutdownHook(new CompositeServiceShutdownHook(master), SHUTDOWN_HOOK_PRIORITY);
      TajoConf conf = new TajoConf(new YarnConfiguration());
      master.init(conf);
      master.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting TajoMaster", t);
      System.exit(-1);
    }
  }
}