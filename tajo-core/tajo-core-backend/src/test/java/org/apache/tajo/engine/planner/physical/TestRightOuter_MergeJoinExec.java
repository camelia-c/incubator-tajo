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

package org.apache.tajo.engine.planner.physical;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.tajo.engine.planner.physical.LeftOuter_HashJoinExec;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRightOuter_MergeJoinExec {
  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestRightOuter_MergeJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private StorageManager sm;
  private Path testDir;

  private TableDesc dep3;
  private TableDesc dep4;
  private TableDesc job3;
  private TableDesc emp3;
  private TableDesc phone3;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    conf = util.getConfiguration();
    sm = StorageManager.get(conf, testDir);

    //----------------- dep3 ------------------------------
    // dep_id | dep_name  | loc_id
    //--------------------------------
    //  0     | dep_0     | 1000
    //  1     | dep_1     | 1001
    //  2     | dep_2     | 1002
    //  3     | dep_3     | 1003
    //  4     | dep_4     | 1004
    //  5     | dep_5     | 1005
    //  6     | dep_6     | 1006
    //  7     | dep_7     | 1007
    //  8     | dep_8     | 1008
    //  9     | dep_9     | 1009
    Schema dep3Schema = new Schema();
    dep3Schema.addColumn("dep_id", Type.INT4);
    dep3Schema.addColumn("dep_name", Type.TEXT);
    dep3Schema.addColumn("loc_id", Type.INT4);


    TableMeta dep3Meta = CatalogUtil.newTableMeta(dep3Schema,
        StoreType.CSV);
    Path dep3Path = new Path(testDir, "dep3.csv");
    Appender appender1 = StorageManager.getAppender(conf, dep3Meta, dep3Path);
    appender1.init();
    Tuple tuple = new VTuple(dep3Meta.getSchema().getColumnNum());
    for (int i = 0; i < 10; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i),
                    DatumFactory.createText("dept_" + i),
                    DatumFactory.createInt4(1000 + i) });
      appender1.addTuple(tuple);
    }

    appender1.flush();
    appender1.close();
    dep3 = CatalogUtil.newTableDesc("dep3", dep3Meta, dep3Path);
    catalog.addTable(dep3);


    //----------------- dep4 ------------------------------
    // dep_id | dep_name  | loc_id
    //--------------------------------
    //  0     | dep_0     | 1000
    //  1     | dep_1     | 1001
    //  2     | dep_2     | 1002
    //  3     | dep_3     | 1003
    //  4     | dep_4     | 1004
    //  5     | dep_5     | 1005
    //  6     | dep_6     | 1006
    //  7     | dep_7     | 1007
    //  8     | dep_8     | 1008
    //  9     | dep_9     | 1009
    // 10     | dep_10    | 1010
    Schema dep4Schema = new Schema();
    dep4Schema.addColumn("dep_id", Type.INT4);
    dep4Schema.addColumn("dep_name", Type.TEXT);
    dep4Schema.addColumn("loc_id", Type.INT4);


    TableMeta dep4Meta = CatalogUtil.newTableMeta(dep4Schema,
        StoreType.CSV);
    Path dep4Path = new Path(testDir, "dep4.csv");
    Appender appender4 = StorageManager.getAppender(conf, dep4Meta, dep4Path);
    appender4.init();
    Tuple tuple4 = new VTuple(dep4Meta.getSchema().getColumnNum());
    for (int i = 0; i < 11; i++) {
      tuple4.put(new Datum[] { DatumFactory.createInt4(i),
                    DatumFactory.createText("dept_" + i),
                    DatumFactory.createInt4(1000 + i) });
      appender4.addTuple(tuple4);
    }

    appender4.flush();
    appender4.close();
    dep4 = CatalogUtil.newTableDesc("dep4", dep4Meta, dep4Path);
    catalog.addTable(dep4);
    


    //----------------- job3 ------------------------------
    //  job_id  | job_title
    // ----------------------
    //   101    |  job_101
    //   102    |  job_102
    //   103    |  job_103

    Schema job3Schema = new Schema();
    job3Schema.addColumn("job_id", Type.INT4);
    job3Schema.addColumn("job_title", Type.TEXT);


    TableMeta job3Meta = CatalogUtil.newTableMeta(job3Schema,
        StoreType.CSV);
    Path job3Path = new Path(testDir, "job3.csv");
    Appender appender2 = StorageManager.getAppender(conf, job3Meta, job3Path);
    appender2.init();
    Tuple tuple2 = new VTuple(job3Meta.getSchema().getColumnNum());
    for (int i = 1; i < 4; i++) {
      int x = 100 + i;
      tuple2.put(new Datum[] { DatumFactory.createInt4(100 + i),
                    DatumFactory.createText("job_" + x) });
      appender2.addTuple(tuple2);
    }

    appender2.flush();
    appender2.close();
    job3 = CatalogUtil.newTableDesc("job3", job3Meta, job3Path);
    catalog.addTable(job3);



    //---------------------emp3 --------------------
    // emp_id  | first_name | last_name | dep_id | salary | job_id
    // ------------------------------------------------------------
    //  11     |  fn_11     |  ln_11    |  1     | 123    | 101
    //  13     |  fn_13     |  ln_13    |  3     | 369    | 103
    //  15     |  fn_15     |  ln_15    |  5     | 615    | null
    //  17     |  fn_17     |  ln_17    |  7     | 861    | null
    //  19     |  fn_19     |  ln_19    |  9     | 1107   | null
    //  21     |  fn_21     |  ln_21    |  1     | 123    | 101
    //  23     |  fn_23     |  ln_23    |  3     | 369    | 103

    Schema emp3Schema = new Schema();
    emp3Schema.addColumn("emp_id", Type.INT4);
    emp3Schema.addColumn("first_name", Type.TEXT);
    emp3Schema.addColumn("last_name", Type.TEXT);
    emp3Schema.addColumn("dep_id", Type.INT4);
    emp3Schema.addColumn("salary", Type.FLOAT4);
    emp3Schema.addColumn("job_id", Type.INT4);


    TableMeta emp3Meta = CatalogUtil.newTableMeta(emp3Schema, StoreType.CSV);
    Path emp3Path = new Path(testDir, "emp3.csv");
    Appender appender3 = StorageManager.getAppender(conf, emp3Meta, emp3Path);
    appender3.init();
    Tuple tuple3 = new VTuple(emp3Meta.getSchema().getColumnNum());

    for (int i = 1; i < 4; i += 2) {
      int x = 10 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(10 + i),
          DatumFactory.createText("firstname_" + x),
          DatumFactory.createText("lastname_" + x),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createInt4(100 + i) });
      appender3.addTuple(tuple3);

      int y = 20 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(20 + i),
          DatumFactory.createText("firstname_" + y),
          DatumFactory.createText("lastname_" + y),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createInt4(100 + i) });
      appender3.addTuple(tuple3);
    }

    for (int i = 5; i < 10; i += 2) {
      int x = 10 + i;
      tuple3.put(new Datum[] { DatumFactory.createInt4(10 + i),
          DatumFactory.createText("firstname_" + x),
          DatumFactory.createText("lastname_" + x),
          DatumFactory.createInt4(i),
          DatumFactory.createFloat4(123 * i),
          DatumFactory.createNullDatum() });
      appender3.addTuple(tuple3);
    }

    appender3.flush();
    appender3.close();
    emp3 = CatalogUtil.newTableDesc("emp3", emp3Meta, emp3Path);
    catalog.addTable(emp3);

    //---------------------phone3 --------------------
    // emp_id  | phone_number
    // -----------------------------------------------
    // this table is empty, no rows

    Schema phone3Schema = new Schema();
    phone3Schema.addColumn("emp_id", Type.INT4);
    phone3Schema.addColumn("phone_number", Type.TEXT);


    TableMeta phone3Meta = CatalogUtil.newTableMeta(phone3Schema,
        StoreType.CSV);
    Path phone3Path = new Path(testDir, "phone3.csv");
    Appender appender5 = StorageManager.getAppender(conf, phone3Meta, phone3Path);
    appender5.init();
    
    appender5.flush();
    appender5.close();
    phone3 = CatalogUtil.newTableDesc("phone3", phone3Meta, phone3Path);
    catalog.addTable(phone3);



    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select dep3.dep_id, dep_name, emp_id, salary from emp3 right outer join dep3 on dep3.dep_id = emp3.dep_id", //0 no nulls
      "select job3.job_id, job_title, emp_id, salary from emp3 right outer join job3 on job3.job_id=emp3.job_id", //1 nulls on the left operand
      "select job3.job_id, job_title, emp_id, salary from job3 right outer join emp3 on job3.job_id=emp3.job_id", //2 nulls on the right side
      "select dep4.dep_id, dep_name, emp_id, salary from emp3 right outer join dep4 on dep4.dep_id = emp3.dep_id", //3 no nulls, right continues after left
      "select emp3.emp_id, first_name, phone_number from emp3 right outer join phone3 on emp3.emp_id = phone3.emp_id", //4 one operand is empty
      "select phone_number, emp3.emp_id, first_name from phone3 right outer join emp3 on emp3.emp_id = phone3.emp_id" //5 one operand is empty
  };

  @Test
  public final void testRightOuter_MergeJoin0() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] dep3Frags = StorageManager.splitNG(conf, "dep3", dep3.getMeta(), dep3.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(emp3Frags, dep3Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin0");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("emp3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("dep_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema dep3Schema = catalog.getTableDesc("dep3").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          dep3Schema.getColumnByName("dep_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());

      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(12, count);
  }


  @Test
  public final void testRightOuter_MergeJoin1() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] job3Frags = StorageManager.splitNG(conf, "job3", job3.getMeta(), job3.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(job3Frags, emp3Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin1");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[1]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("emp3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("job_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema job3Schema = catalog.getTableDesc("job3").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          job3Schema.getColumnByName("job_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());

      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(5, count);
  }

  @Test
  public final void testRightOuter_MergeJoin2() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] job3Frags = StorageManager.splitNG(conf, "job3", job3.getMeta(), job3.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(job3Frags, emp3Frags);


    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin2");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[2]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("job3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema job3Schema = catalog.getTableDesc("job3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          job3Schema.getColumnByName("job_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("job_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());

      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(7, count);
  }


  @Test
  public final void testRightOuter_MergeJoin3() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] dep4Frags = StorageManager.splitNG(conf, "dep4", dep4.getMeta(), dep4.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(emp3Frags, dep4Frags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin3");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[3]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("emp3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("dep_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema dep4Schema = catalog.getTableDesc("dep4").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          dep4Schema.getColumnByName("dep_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());

      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(13, count);
  }

    @Test
  public final void testRightOuter_MergeJoin4() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] phone3Frags = StorageManager.splitNG(conf, "phone3", phone3.getMeta(), phone3.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(emp3Frags, phone3Frags);


    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin4");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[4]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("emp3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("emp_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema phone3Schema = catalog.getTableDesc("phone3").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          phone3Schema.getColumnByName("emp_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());


      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(0, count);
  }

   @Test
  public final void testRightOuter_MergeJoin5() throws IOException, PlanningException {
    Fragment[] emp3Frags = StorageManager.splitNG(conf, "emp3", emp3.getMeta(), emp3.getPath(),
        Integer.MAX_VALUE);
    Fragment[] phone3Frags = StorageManager.splitNG(conf, "phone3", phone3.getMeta(), phone3.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(phone3Frags,emp3Frags);


    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testRightOuter_MergeJoin5");
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        TUtil.newQueryUnitAttemptId(), merged, workDir);
    Expr expr = analyzer.parse(QUERIES[5]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, plan);

    ProjectionExec proj = (ProjectionExec) exec;

    // if it chose the hash join WITH REVERSED ORDER, convert to merge join exec
    if (!(proj.getChild() instanceof RightOuter_MergeJoinExec)) {
      //TODO
      BinaryPhysicalExec aJoin = (BinaryPhysicalExec) proj.getChild();
      SeqScanExec outerScan = (SeqScanExec) aJoin.getRightChild(); //correct is reversed
      SeqScanExec innerScan = (SeqScanExec) aJoin.getLeftChild();

      SeqScanExec tmp;
      if (!outerScan.getTableName().equals("phone3")) {
        tmp = outerScan;
        outerScan = innerScan;
        innerScan = tmp;
      }

      SortSpec[] outerSortKeys = new SortSpec[1];
      SortSpec[] innerSortKeys = new SortSpec[1];

      Schema phone3Schema = catalog.getTableDesc("phone3").getMeta()
          .getSchema();
      outerSortKeys[0] = new SortSpec(
          phone3Schema.getColumnByName("emp_id"));
      SortNode outerSort = new SortNode(outerSortKeys);
      outerSort.setInSchema(outerScan.getSchema());
      outerSort.setOutSchema(outerScan.getSchema());

      Schema emp3Schema = catalog.getTableDesc("emp3").getMeta().getSchema();
      innerSortKeys[0] = new SortSpec(
          emp3Schema.getColumnByName("emp_id"));
      SortNode innerSort = new SortNode(innerSortKeys);
      innerSort.setInSchema(innerScan.getSchema());
      innerSort.setOutSchema(innerScan.getSchema());


      MemSortExec outerSortExec = new MemSortExec(ctx, outerSort, outerScan);
      MemSortExec innerSortExec = new MemSortExec(ctx, innerSort, innerScan);

      RightOuter_MergeJoinExec mergeJoin = new RightOuter_MergeJoinExec(ctx,
          ((LeftOuter_HashJoinExec)aJoin).getPlan(), outerSortExec, innerSortExec,
          outerSortKeys, innerSortKeys);
      proj.setChild(mergeJoin);
      exec = proj;
    }

    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
  
    while ((tuple = exec.next()) != null) {
       //TODO check contents
       count = count + 1;
    }
    exec.close();
    assertEquals(7, count);
  }

  

}
