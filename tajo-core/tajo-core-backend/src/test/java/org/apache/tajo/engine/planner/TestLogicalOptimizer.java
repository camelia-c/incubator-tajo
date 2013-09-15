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

package org.apache.tajo.engine.planner;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.TajoMaster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Test;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.planner.OuterJoinMetadata;
import org.apache.tajo.engine.utils.OuterJoinUtil;

import static org.junit.Assert.*;

public class TestLogicalOptimizer {

  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer sqlAnalyzer;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }
    
    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empId", Type.INT4);
    schema.addColumn("deptName", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema schema3 = new Schema();
    schema3.addColumn("deptName", Type.TEXT);
    schema3.addColumn("score", Type.INT4);
    schema3.addColumn("phone", Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    TableDesc people = new TableDescImpl("employee", meta,
        new Path("file:///"));
    catalog.addTable(people);

    TableDesc student = new TableDescImpl("dept", schema2, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(student);

    TableDesc score = new TableDescImpl("score", schema3, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.GENERAL,
        CatalogUtil.newDataTypesWithoutLen(Type.INT4),
        CatalogUtil.newDataTypesWithoutLen(Type.INT4));

    catalog.registerFunction(funcDesc);

    //camelia --
    /*
    create external table reg1 (reg_id int, reg_name text) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/REG1';
    */
    Schema schema14 = new Schema();
    schema14.addColumn("reg_id", Type.INT4);
    schema14.addColumn("reg_name", Type.TEXT);

    TableDesc region2 = new TableDescImpl("region2", schema14, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(region2);


    /*
    create external table country1 (country_id int, country_name text, reg_id int) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/COUNTRY1';
    */
    Schema schema15 = new Schema();
    schema15.addColumn("country_id", Type.INT4);
    schema15.addColumn("country_name", Type.TEXT);
    schema15.addColumn("reg_id", Type.INT4);

    TableDesc country2 = new TableDescImpl("country2", schema15, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(country2);


    /*
    create external table loc1 (id int, city text, country_id int) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/LOC1';
    */
    Schema schema16 = new Schema();
    schema16.addColumn("id", Type.INT4);
    schema16.addColumn("city", Type.TEXT);
    schema16.addColumn("country_id", Type.INT4);

    TableDesc loc2 = new TableDescImpl("loc2", schema16, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(loc2);

    /*
    create external table shop1 (shop_id int, shop_name text, loc_id int) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/SHOP1';
    */
    Schema schema17 = new Schema();
    schema17.addColumn("shop_id", Type.INT4);
    schema17.addColumn("shop_name", Type.TEXT);
    schema17.addColumn("loc_id", Type.INT4);

    TableDesc shop2 = new TableDescImpl("shop2", schema17, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(shop2);



    /*
    create external table dep1 (dep_id int, dep_name text, loc_id int) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/DEP1';
    */
    Schema schema18 = new Schema();
    schema18.addColumn("dep_id", Type.INT4);
    schema18.addColumn("dep_name", Type.TEXT);
    schema18.addColumn("loc_id", Type.INT4);

    TableDesc dep2 = new TableDescImpl("dep2", schema18, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(dep2);


    /*
    create external table job1 (job_id int, job_title text) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/JOB1';
    */
    Schema schema19 = new Schema();
    schema19.addColumn("job_id", Type.INT4);
    schema19.addColumn("job_title", Type.TEXT);

    TableDesc job2 = new TableDescImpl("job2", schema19, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(job2);

    /*
    create external table emp1 (emp_id int, first_name text, last_name text, dep_id int, salary float, job_id int) using csv with ('csvfile.delimiter'=',') location 'file:/home/camelia/tajo_git/date_test/EMP1';
    */
    Schema schema20 = new Schema();
    schema20.addColumn("emp_id", Type.INT4);
    schema20.addColumn("first_name", Type.TEXT);
    schema20.addColumn("last_name", Type.TEXT);
    schema20.addColumn("dep_id", Type.INT4);
    schema20.addColumn("salary", Type.FLOAT4);
    schema20.addColumn("job_id", Type.INT4);

    TableDesc emp2 = new TableDescImpl("emp2", schema20, StoreType.CSV,
        new Options(),
        new Path("file:///"));
    catalog.addTable(emp2);

    //--camelia

    sqlAnalyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @After
  public void resetOuterUtil(){
     OuterJoinUtil.resetOuterJoinUtil();
  }
  
  static String[] QUERIES = {
    "select name, manager from employee as e, dept as dp where e.deptName = dp.deptName", // 0
    "select name, empId, deptName from employee where empId > 500", // 1
    "select name from employee where empId = 100", // 2
    "select name, max(empId) as final from employee where empId > 50 group by name", // 3
    "select name, score from employee natural join score", // 4
    "select name, score from employee join score on employee.deptName = score.deptName", // 5      
  };

  //camelia--
  static String[] OUTERJOINQUERIES = {
     "select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id", // 0 multinullsupplier0
     "select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id", //1 multinullsupplier1
    "select city , dep_name , shop_name  from  loc2 right outer join dep2 on loc2.id=dep2.loc_id right outer join shop2 on loc2.id=shop2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id", //2 multinullsupplier2
    "select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id left outer join shop2 on loc2.id=shop2.loc_id", //3 multinullsupplier3
    "select dep_name, salary from dep2 left outer join emp2 on dep2.dep_id=emp2.dep_id where emp2.salary > 100",//4 restrictednullsupplier0
    "select dep_name, salary from dep2 left outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500",//5 restrictednullsupplier1
    "select dep_name, salary from dep2 right outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500",//6 restrictednullsupplier2
    "select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where emp2.salary > 100",//7 restrictednullsupplier3
    "select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500",//8 restrictednullsupplier4
    "select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500 and emp2.salary > 100",//9 restrictednullsupplier5
    "select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id left outer join emp2 on emp2.dep_id=dep2.dep_id where dep2.loc_id < 500", //10 restrictednullsupplier6
     "select city , dep_name , salary  from  country2 right outer join loc2 on country2.country_id=loc2.country_id right outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id right outer join job2 on emp2.job_id=job2.job_id where country2.country_id>10", //11 restrictednullsupplier7
    "select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id left outer join emp2 on emp2.dep_id=dep2.dep_id join job2 on emp2.job_id=job2.job_id", //12 restrictednullsupplier8
    "select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id , emp2 where emp2.dep_id=dep2.dep_id", //13 restrictednullsupplier9   
    "select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where salary in (123.0, 369.0)", //14 restrictednullsupplier10 
    "select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where salary not in (123.0, 369.0)", //15 restrictednullsupplier11
    "select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where first_name like '%_%'", //16 restrictednullsupplier12
    "select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id left outer join shop2 on loc2.id=shop2.loc_id where shop2.shop_id>10" ,//17 combined1
    "select city , dep_name , shop_name  from  loc2 right outer join dep2 on loc2.id=dep2.loc_id right outer join shop2 on loc2.id=shop2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id right outer join job2 on emp2.job_id=job2.job_id where emp2.emp_id>10",  //18 combined2
    "select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id full outer join emp2 on emp2.dep_id=dep2.dep_id left outer join job2 on emp2.job_id=job2.job_id where job2.job_id > 10", //19 combined3 
  };

  //////////////////// multi null suppliers \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

  @Test
  public final void testMultiNullSupplier0() throws PlanningException, CloneNotSupportedException {
   /* "select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id"  */
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[0]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode2.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, joinNode2.getLeftChild().getType()); //SHOULD BE LOC2
    
    

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
  
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);//?
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, ajoinNode2.getLeftChild().getType()); //SHOULD BE LOC2

    

  }


  @Test
  public final void testMultiNullSupplier1() throws PlanningException, CloneNotSupportedException {
     /*"select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id"*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[1]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode2.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode3.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, joinNode3.getLeftChild().getType()); //SHOULD BE COUNTRY2
    
    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, ajoinNode3.getLeftChild().getType()); //SHOULD BE COUNTRY2

    
  }

  @Test
  public final void testMultiNullSupplier2() throws PlanningException, CloneNotSupportedException {
     /*"select city , dep_name , shop_name  from  loc2 right outer join dep2 on loc2.id=dep2.loc_id right outer join shop2 on loc2.id=shop2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id"*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[2]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode2.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE SHOP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode3.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, joinNode3.getLeftChild().getType()); //SHOULD BE LOC2
    
    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }

    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode2.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE SHOP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, ajoinNode3.getLeftChild().getType()); //SHOULD BE LOC2

    
    

  }
  
  @Test
  public final void testMultiNullSupplier3() throws PlanningException, CloneNotSupportedException {
   /*
    "select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id left outer join shop2 on loc2.id=shop2.loc_id"
   */
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[3]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE SHOP2
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode2.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode3.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE DEP2
    JoinNode joinNode4 = (JoinNode) joinNode3.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode4.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode4.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, joinNode4.getLeftChild().getType()); //SHOULD BE COUNTRY2
    
    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.LEFT_OUTER, ajoinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE SHOP2
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode2.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE DEP2
    JoinNode ajoinNode4 = (JoinNode) ajoinNode3.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode4.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode4.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, ajoinNode4.getLeftChild().getType()); //SHOULD BE COUNTRY2
   
    
    
  }

  //////////////////// restricted null supplier SELECTION-BASED \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  
  @Test
  public final void testRestrictedNullSupplier0() throws PlanningException, CloneNotSupportedException {
  /*  "select dep_name, salary from dep2 left outer join emp2 on dep2.dep_id=emp2.dep_id where emp2.salary > 100"*/ 
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[4]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    
  }

  @Test
  public final void testRestrictedNullSupplier1() throws PlanningException, CloneNotSupportedException {
  /*  select dep_name, salary from dep2 left outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500 
   --> SHOULD REMAIN UNCHANGED*/ 
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[5]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);
    //--> SHOULD REMAIN UNCHANGED
    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.LEFT_OUTER, ajoinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    

  }

  @Test
  public final void testRestrictedNullSupplier2() throws PlanningException, CloneNotSupportedException {
  /*select dep_name, salary from dep2 right outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500 */
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[6]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    
  }

  @Test
  public final void testRestrictedNullSupplier3() throws PlanningException, CloneNotSupportedException {
  /*"select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where emp2.salary > 100" */
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[7]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.FULL_OUTER, joinNode.getJoinType());          //FULL_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.RIGHT_OUTER, ajoinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    
  }

  @Test
  public final void testRestrictedNullSupplier4() throws PlanningException, CloneNotSupportedException {
    /*"select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500 "*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[8]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.FULL_OUTER, joinNode.getJoinType());          //FULL_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.LEFT_OUTER, ajoinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    
    
  }

  @Test
  public final void testRestrictedNullSupplier5() throws PlanningException, CloneNotSupportedException {
    /*select dep_name, salary from dep2 full outer join emp2 on dep2.dep_id=emp2.dep_id where dep2.loc_id < 500 and emp2.salary > 100*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[9]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.FULL_OUTER, joinNode.getJoinType());          //FULL_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
    
  }
  
  @Test
  public final void testRestrictedNullSupplier6() throws PlanningException, CloneNotSupportedException {
    /*select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id left outer join emp2 on emp2.dep_id=dep2.dep_id where dep2.loc_id < 500*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[10]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2    
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode2.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, joinNode2.getLeftChild().getType()); //SHOULD BE LOC2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.LEFT_OUTER, ajoinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2    
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, ajoinNode2.getLeftChild().getType()); //SHOULD BE LOC2

    
    
  }

  @Test
  public final void testRestrictedNullSupplier7() throws PlanningException, CloneNotSupportedException {
  
    /*"select city , dep_name , salary  from  country2 right outer join loc2 on country2.country_id=loc2.country_id right outer join dep2 on loc2.id=dep2.loc_id right outer join emp2 on emp2.dep_id=dep2.dep_id right outer join job2 on emp2.job_id=job2.job_id where country2.country_id>10", */

    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[11]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode.getJoinType());          //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE JOB2    
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode2.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode3.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, joinNode3.getLeftChild().getType());
    JoinNode joinNode4 = (JoinNode) joinNode3.getLeftChild();
    assertEquals(JoinType.RIGHT_OUTER, joinNode4.getJoinType()); //RIGHT_OUTER
    assertEquals(NodeType.SCAN, joinNode4.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, joinNode4.getLeftChild().getType()); //SHOULD BE COUNTRY2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE JOB2    
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, ajoinNode3.getLeftChild().getType());
    JoinNode ajoinNode4 = (JoinNode) ajoinNode3.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode4.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode4.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, ajoinNode4.getLeftChild().getType()); //SHOULD BE COUNTRY2

    
    
  }

  //////////////////// restricted null supplier INNER JOIN-BASED \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
  
  @Test
  public final void testRestrictedNullSupplier8() throws PlanningException, CloneNotSupportedException {
  
    /*select city , dep_name , salary  from  loc2 left outer join dep2 on loc2.id=dep2.loc_id left outer join emp2 on emp2.dep_id=dep2.dep_id join job2 on emp2.job_id=job2.job_id*/

    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[12]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(JoinType.INNER, joinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE JOB2    
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode2.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode3.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, joinNode3.getLeftChild().getType()); //SHOULD BE LOC2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE JOB2    
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.SCAN, ajoinNode3.getLeftChild().getType()); //SHOULD BE LOC2
    
    
  }

  @Test
  public final void testRestrictedNullSupplier9() throws PlanningException, CloneNotSupportedException {
    /* select city , dep_name , salary  from  country2 left outer join loc2 on country2.country_id=loc2.country_id left outer join dep2 on loc2.id=dep2.loc_id , emp2 where emp2.dep_id=dep2.dep_id */

    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[13]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.CROSS, joinNode.getJoinType());          //CROSS
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2    
    assertEquals(NodeType.JOIN, joinNode.getLeftChild().getType());
    JoinNode joinNode2 = (JoinNode) joinNode.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode2.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, joinNode2.getLeftChild().getType());
    JoinNode joinNode3 = (JoinNode) joinNode2.getLeftChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode3.getJoinType()); //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode3.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, joinNode3.getLeftChild().getType()); //SHOULD BE COUNTRY2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2    
    assertEquals(NodeType.JOIN, ajoinNode.getLeftChild().getType());
    JoinNode ajoinNode2 = (JoinNode) ajoinNode.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode2.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode2.getRightChild().getType()); //SHOULD BE DEP2
    assertEquals(NodeType.JOIN, ajoinNode2.getLeftChild().getType());
    JoinNode ajoinNode3 = (JoinNode) ajoinNode2.getLeftChild();
    assertEquals(JoinType.INNER, ajoinNode3.getJoinType()); //INNER
    assertEquals(NodeType.SCAN, ajoinNode3.getRightChild().getType()); //SHOULD BE LOC2
    assertEquals(NodeType.SCAN, ajoinNode3.getLeftChild().getType()); //SHOULD BE COUNTRY2
    
    

  }
  
  /////////////////////////////  restrictednullsupplier with IN, LIKE predicates \\\\\\\\\\\\\\\\\\\
  
  @Test
  public final void testRestrictedNullSupplier10() throws PlanningException, CloneNotSupportedException {
    /*select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where salary in (123.0, 369.0)*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[14]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
  }

  
  @Test
  public final void testRestrictedNullSupplier11() throws PlanningException, CloneNotSupportedException {
    /*select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where salary not in (123.0, 369.0)*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[15]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
  }
 
    @Test
  public final void testRestrictedNullSupplier13() throws PlanningException, CloneNotSupportedException {
    /*select dep2.dep_id, dep_name, emp_id, salary from dep2 left outer join emp2 on dep2.dep_id = emp2.dep_id where first_name like '%_%'*/
    Expr expr = sqlAnalyzer.parse(OUTERJOINQUERIES[16]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertEquals(JoinType.LEFT_OUTER, joinNode.getJoinType());          //LEFT_OUTER
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType()); //SHOULD BE DEP2

    try{
       LogicalRootNode rootNode = (LogicalRootNode) newPlan.getRootBlock().getRoot();
       if ((PlannerUtil.checkIfDDLPlan(rootNode) == false) && (PlannerUtil.checkIfDMLPlan(rootNode) == false)){
          OuterJoinMetadata ojmeta = new OuterJoinMetadata(newPlan);
       }
    } catch(org.apache.tajo.engine.planner.PlanningException ex) {
      
    }
    optimizer.optimize(newPlan);

    LogicalNode aplan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, aplan.getType());   
    LogicalRootNode aroot = (LogicalRootNode) aplan;
    TestLogicalNode.testCloneLogicalNode(aroot);
    assertEquals(NodeType.JOIN, aroot.getChild().getType());
    JoinNode ajoinNode = (JoinNode) aroot.getChild();
    assertEquals(JoinType.INNER, ajoinNode.getJoinType());          //INNER
    assertEquals(NodeType.SCAN, ajoinNode.getRightChild().getType()); //SHOULD BE EMP2
    assertEquals(NodeType.SCAN, ajoinNode.getLeftChild().getType()); //SHOULD BE DEP2

    
  }

  
  ///////////////////////////// combined with multinullsupplier and restrictednullsupplier \\\\\\\\\\\\\\\\\\\
  //TODO TEST FOR 17,18,19  



  //--camelia
  
  @Test
  public final void testProjectionPushWithNaturalJoin() throws PlanningException, CloneNotSupportedException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[4]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    
    LogicalNode optimized = optimizer.optimize(newPlan);

    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.JOIN, root.getChild().getType());
    joinNode = (JoinNode) root.getChild();
    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
  }
  
  @Test
  public final void testProjectionPushWithInnerJoin() throws PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[5]);
    LogicalPlan newPlan = planner.createPlan(expr);
    optimizer.optimize(newPlan);
  }
  
  @Test
  public final void testProjectionPush() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[2]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    assertEquals(NodeType.SCAN, selNode.getChild().getType());

    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.SCAN, root.getChild().getType());
  }
  
  @Test
  public final void testOptimizeWithGroupBy() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(QUERIES[3]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
        
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();
    assertEquals(NodeType.GROUP_BY, projNode.getChild().getType());
    GroupbyNode groupbyNode = (GroupbyNode) projNode.getChild();
    assertEquals(NodeType.SELECTION, groupbyNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) groupbyNode.getChild();
    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    TestLogicalNode.testCloneLogicalNode(root);
    assertEquals(NodeType.GROUP_BY, root.getChild().getType());
    groupbyNode = (GroupbyNode) root.getChild();
    assertEquals(NodeType.SCAN, groupbyNode.getChild().getType());
  }

  @Test
  public final void testPushable() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = sqlAnalyzer.parse(QUERIES[0]);
    LogicalPlan newPlan = planner.createPlan(expr);
    LogicalNode plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    SelectionNode selNode = (SelectionNode) projNode.getChild();
    
    assertEquals(NodeType.JOIN, selNode.getChild().getType());
    JoinNode joinNode = (JoinNode) selNode.getChild();
    assertFalse(joinNode.hasJoinQual());
    
    // Test for Pushable
    assertTrue(PlannerUtil.canBeEvaluated(selNode.getQual(), joinNode));
    
    // Optimized plan
    LogicalNode optimized = optimizer.optimize(newPlan);
    assertEquals(NodeType.ROOT, optimized.getType());
    root = (LogicalRootNode) optimized;
    
    assertEquals(NodeType.JOIN, root.getChild().getType());
    joinNode = (JoinNode) root.getChild();
    assertTrue(joinNode.hasJoinQual());
    
    // Scan Pushable Test
    expr = sqlAnalyzer.parse(QUERIES[1]);
    newPlan = planner.createPlan(expr);
    plan = newPlan.getRootBlock().getRoot();
    
    assertEquals(NodeType.ROOT, plan.getType());
    root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    projNode = (ProjectionNode) root.getChild();

    assertEquals(NodeType.SELECTION, projNode.getChild().getType());
    selNode = (SelectionNode) projNode.getChild();
    
    assertEquals(NodeType.SCAN, selNode.getChild().getType());
    ScanNode scanNode = (ScanNode) selNode.getChild();
    // Test for Join Node
    assertTrue(PlannerUtil.canBeEvaluated(selNode.getQual(), scanNode));
  }

  @Test
  public final void testInsertInto() throws CloneNotSupportedException, PlanningException {
    Expr expr = sqlAnalyzer.parse(TestLogicalPlanner.insertStatements[0]);
    LogicalPlan newPlan = planner.createPlan(expr);
    optimizer.optimize(newPlan);
  }
}
