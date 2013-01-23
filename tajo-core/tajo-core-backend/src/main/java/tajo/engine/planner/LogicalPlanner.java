/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.CatalogService;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.engine.eval.*;
import tajo.engine.parser.*;
import tajo.engine.parser.QueryBlock.*;
import tajo.engine.planner.logical.*;
import tajo.engine.planner.logical.join.Edge;
import tajo.engine.planner.logical.join.JoinTree;
import tajo.engine.query.exception.InvalidQueryException;
import tajo.engine.query.exception.NotSupportQueryException;
import tajo.engine.utils.SchemaUtil;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * This class creates a logical plan from a parse tree ({@link tajo.engine.parser.QueryBlock})
 * generated by {@link tajo.engine.parser.QueryAnalyzer}.
 *
 * @see tajo.engine.parser.QueryBlock
 */
public class LogicalPlanner {
  private static Log LOG = LogFactory.getLog(LogicalPlanner.class);
  private final CatalogService catalog;

  public LogicalPlanner(CatalogService catalog) {
    this.catalog = catalog;
  }

  /**
   * This generates a logical plan.
   *
   * @param context
   * @return a initial logical plan
   */
  public LogicalNode createPlan(PlanningContext context) {
    LogicalNode plan;

    try {
      plan = createPlanInternal(context, context.getParseTree());
    } catch (CloneNotSupportedException e) {
      throw new InvalidQueryException(e);
    }

    LogicalRootNode root = new LogicalRootNode();
    root.setInSchema(plan.getOutSchema());
    root.setOutSchema(plan.getOutSchema());
    root.setSubNode(plan);

    return root;
  }
  
  private LogicalNode createPlanInternal(PlanningContext ctx,
      ParseTree query) throws CloneNotSupportedException {
    LogicalNode plan;
    
    switch(query.getType()) {
    case SELECT:
      LOG.info("Planning select statement");
      QueryBlock select = (QueryBlock) query;
      plan = buildSelectPlan(ctx, select);
      break;
      
    case UNION:
    case EXCEPT:
    case INTERSECT:
      SetStmt set = (SetStmt) query;
      plan = buildSetPlan(ctx, set);
      break;
      
    case CREATE_INDEX:
      LOG.info("Planning create index statement");
      CreateIndexStmt createIndex = (CreateIndexStmt) query;
      plan = buildCreateIndexPlan(createIndex);
      break;

    case CREATE_TABLE:
    case CREATE_TABLE_AS:
      LOG.info("Planning store statement");
      CreateTableStmt createTable = (CreateTableStmt) query;
      plan = buildCreateTablePlan(ctx, createTable);
      break;

    default:
      throw new NotSupportQueryException(query.toString());
    }
    
    return plan;
  }

  private LogicalNode buildSetPlan(PlanningContext ctx, SetStmt stmt)
      throws CloneNotSupportedException {
    BinaryNode bin;
    switch (stmt.getType()) {
    case UNION:
      bin = new UnionNode();
      break;
    case EXCEPT:
      bin = new ExceptNode();
      break;
    case INTERSECT:
      bin = new IntersectNode();
      break;
    default:
      throw new IllegalStateException("the statement cannot be matched to any set operation type");
    }
    
    bin.setOuter(createPlanInternal(ctx, stmt.getLeftTree()));
    bin.setInner(createPlanInternal(ctx, stmt.getRightTree()));
    bin.setInSchema(bin.getOuterNode().getOutSchema());
    bin.setOutSchema(bin.getOuterNode().getOutSchema());
    return bin;
  }

  private LogicalNode buildCreateIndexPlan(CreateIndexStmt stmt) {
    FromTable table = new FromTable(catalog.getTableDesc(stmt.getTableName()));
    ScanNode scan = new ScanNode(table);
    scan.setInSchema(table.getSchema());
    scan.setOutSchema(table.getSchema());
    IndexWriteNode indexWrite = new IndexWriteNode(stmt);
    indexWrite.setSubNode(scan);
    indexWrite.setInSchema(scan.getOutSchema());
    indexWrite.setOutSchema(scan.getOutSchema());
    
    return indexWrite;
  }

  private static LogicalNode buildCreateTablePlan(final PlanningContext ctx,
                                                  final CreateTableStmt query)
      throws CloneNotSupportedException {
    LogicalNode node;

    if (query.hasQueryBlock()) {
      LogicalNode selectPlan = buildSelectPlan(ctx, query.getSelectStmt());
      StoreTableNode storeNode = new StoreTableNode(query.getTableName());

      storeNode.setSubNode(selectPlan);

      if (query.hasDefinition()) {
        storeNode.setOutSchema(query.getTableDef());
      } else {
        // TODO - strip qualified name
        storeNode.setOutSchema(selectPlan.getOutSchema());
      }
      storeNode.setInSchema(selectPlan.getOutSchema());

      if (query.hasStoreType()) {
        storeNode.setStorageType(query.getStoreType());
      } else {
        // default type
        // TODO - it should be configurable.
        storeNode.setStorageType(CatalogProtos.StoreType.CSV);
      }
      if (query.hasOptions()) {
        storeNode.setOptions(query.getOptions());
      }

      node = storeNode;
    } else {
      CreateTableNode createTable =
          new CreateTableNode(query.getTableName(), query.getTableDef());

      if (query.hasStoreType()) {
        createTable.setStorageType(query.getStoreType());
      } else {
        // default type
        // TODO - it should be configurable.
        createTable.setStorageType(CatalogProtos.StoreType.CSV);
      }
      if (query.hasOptions()) {
        createTable.setOptions(query.getOptions());
      }

      if (query.hasPath()) {
        createTable.setPath(query.getPath());
      }

      node = createTable;
    }
    
    return node;
  }
  
  /**
   * ^(SELECT from_clause? where_clause? groupby_clause? selectList)
   * 
   * @param query
   * @return the planed logical plan
   */
  private static LogicalNode buildSelectPlan(PlanningContext ctx,
                                             QueryBlock query)
      throws CloneNotSupportedException {
    LogicalNode subroot;
    EvalNode whereCondition = null;
    EvalNode [] cnf = null;
    if(query.hasWhereClause()) {
      whereCondition = query.getWhereCondition();
      whereCondition = AlgebraicUtil.simplify(whereCondition);
      cnf = EvalTreeUtil.getConjNormalForm(whereCondition);
    }

    if(query.hasFromClause()) {
      if (query.hasExplicitJoinClause()) {
        subroot = createExplicitJoinTree(query);
      } else {
        subroot = createImplicitJoinTree(query.getFromTables(), cnf);
      }
    } else {
      subroot = new EvalExprNode(query.getTargetList());
      subroot.setOutSchema(getProjectedSchema(ctx, query.getTargetList()));
      return subroot;
    }
    
    if(whereCondition != null) {
      SelectionNode selNode = 
          new SelectionNode(query.getWhereCondition());
      selNode.setSubNode(subroot);
      selNode.setInSchema(subroot.getOutSchema());
      selNode.setOutSchema(selNode.getInSchema());
      subroot = selNode;
    }
    
    if(query.hasAggregation()) {
      if (query.isDistinct()) {
        throw new InvalidQueryException("Cannot support GROUP BY queries with distinct keyword");
      }

      GroupbyNode groupbyNode = null;
      if (query.hasGroupbyClause()) {
        if (query.getGroupByClause().getGroupSet().get(0).getType() == GroupType.GROUPBY) {          
          groupbyNode = new GroupbyNode(query.getGroupByClause().getGroupSet().get(0).getColumns());
          groupbyNode.setTargetList(query.getTargetList());
          groupbyNode.setSubNode(subroot);
          groupbyNode.setInSchema(subroot.getOutSchema());
          Schema outSchema = getProjectedSchema(ctx, query.getTargetList());
          groupbyNode.setOutSchema(outSchema);
          subroot = groupbyNode;
        } else if (query.getGroupByClause().getGroupSet().get(0).getType() == GroupType.CUBE) {
          LogicalNode union = createGroupByUnionByCube(ctx, query,
              subroot, query.getGroupByClause());
          Schema outSchema = getProjectedSchema(ctx, query.getTargetList());
          union.setOutSchema(outSchema);
          subroot = union;
        }
        if(query.hasHavingCond())
          groupbyNode.setHavingCondition(query.getHavingCond());
      } else {
        // when aggregation functions are used without grouping fields
        groupbyNode = new GroupbyNode(new Column[] {});
        groupbyNode.setTargetList(query.getTargetList());
        groupbyNode.setSubNode(subroot);
        groupbyNode.setInSchema(subroot.getOutSchema());
        Schema outSchema = getProjectedSchema(ctx, query.getTargetList());
        groupbyNode.setOutSchema(outSchema);
        subroot = groupbyNode;
      }
    }
    
    if(query.hasOrderByClause()) {
      SortNode sortNode = new SortNode(query.getSortKeys());
      sortNode.setSubNode(subroot);
      sortNode.setInSchema(subroot.getOutSchema());
      sortNode.setOutSchema(sortNode.getInSchema());
      subroot = sortNode;
    }

    ProjectionNode prjNode;
    if (query.getProjectAll()) {
      Schema merged = SchemaUtil.merge(query.getFromTables());
      Target [] allTargets = PlannerUtil.schemaToTargets(merged);
      prjNode = new ProjectionNode(allTargets);
      prjNode.setSubNode(subroot);
      prjNode.setInSchema(merged);
      prjNode.setOutSchema(merged);
      subroot = prjNode;
      query.setTargetList(allTargets);
    } else {
      prjNode = new ProjectionNode(query.getTargetList());
      if (subroot != null) { // false if 'no from' statement
        prjNode.setSubNode(subroot);
      }
      prjNode.setInSchema(subroot.getOutSchema());

      // All aggregate functions are evaluated before the projection.
      // So, the targets for aggregate functions should be updated.
      LogicalOptimizer.TargetListManager tlm = new LogicalOptimizer.
          TargetListManager(ctx, query.getTargetList());
      for (int i = 0; i < tlm.getTargets().length; i++) {
        if (EvalTreeUtil.findDistinctAggFunction(tlm.getTarget(i).getEvalTree()).size() > 0) {
          tlm.setEvaluated(i);
        }
      }
      prjNode.setTargetList(tlm.getUpdatedTarget());
      Schema projected = getProjectedSchema(ctx, tlm.getUpdatedTarget());
      prjNode.setOutSchema(projected);
      subroot = prjNode;
    }

    GroupbyNode dupRemoval;
    if (query.isDistinct()) {
      dupRemoval = new GroupbyNode(subroot.getOutSchema().toArray());
      dupRemoval.setTargetList(query.getTargetList());
      dupRemoval.setSubNode(subroot);
      dupRemoval.setInSchema(subroot.getOutSchema());
      Schema outSchema = getProjectedSchema(ctx, query.getTargetList());
      dupRemoval.setOutSchema(outSchema);
      subroot = dupRemoval;
    }

    if (query.hasLimitClause()) {
      LimitNode limitNode = new LimitNode(query.getLimitClause());
      limitNode.setSubNode(subroot);
      limitNode.setInSchema(subroot.getOutSchema());
      limitNode.setOutSchema(limitNode.getInSchema());
      subroot = limitNode;
    }
    
    return subroot;
  }

  public static LogicalNode createGroupByUnionByCube(
      final PlanningContext context,
      final QueryBlock queryBlock,
      final LogicalNode subNode,
      final GroupByClause clause) {

    GroupElement element = clause.getGroupSet().get(0);

    List<Column []> cuboids  = generateCuboids(element.getColumns());

    return createGroupByUnion(context, queryBlock, subNode, cuboids, 0);
  }

  private static Target [] cloneTargets(Target [] srcs)
      throws CloneNotSupportedException {
    Target [] clone = new Target[srcs.length];
    for (int i = 0; i < srcs.length; i++) {
      clone[i] = (Target) srcs[i].clone();
    }

    return clone;
  }

  private static UnionNode createGroupByUnion(final PlanningContext context,
                                              final QueryBlock queryBlock,
                                              final LogicalNode subNode,
                                              final List<Column []> cuboids,
                                              final int idx) {
    UnionNode union;
    try {
      if ((cuboids.size() - idx) > 2) {
        GroupbyNode g1 = new GroupbyNode(cuboids.get(idx));
        Target [] clone = cloneTargets(queryBlock.getTargetList());

        g1.setTargetList(clone);
        g1.setSubNode((LogicalNode) subNode.clone());
        g1.setInSchema(g1.getSubNode().getOutSchema());
        Schema outSchema = getProjectedSchema(context, queryBlock.getTargetList());
        g1.setOutSchema(outSchema);

        union = new UnionNode(g1, createGroupByUnion(context, queryBlock,
            subNode, cuboids, idx+1));
        union.setInSchema(g1.getOutSchema());
        union.setOutSchema(g1.getOutSchema());
        return union;
      } else {
        GroupbyNode g1 = new GroupbyNode(cuboids.get(idx));
        Target [] clone = cloneTargets(queryBlock.getTargetList());
        g1.setTargetList(clone);
        g1.setSubNode((LogicalNode) subNode.clone());
        g1.setInSchema(g1.getSubNode().getOutSchema());
        Schema outSchema = getProjectedSchema(context, queryBlock.getTargetList());
        g1.setOutSchema(outSchema);

        GroupbyNode g2 = new GroupbyNode(cuboids.get(idx+1));
        clone = cloneTargets(queryBlock.getTargetList());
        g2.setTargetList(clone);
        g2.setSubNode((LogicalNode) subNode.clone());
        g2.setInSchema(g1.getSubNode().getOutSchema());
        outSchema = getProjectedSchema(context, queryBlock.getTargetList());
        g2.setOutSchema(outSchema);
        union = new UnionNode(g1, g2);
        union.setInSchema(g1.getOutSchema());
        union.setOutSchema(g1.getOutSchema());
        return union;
      }
    } catch (CloneNotSupportedException cnse) {
      LOG.error(cnse);
      throw new InvalidQueryException(cnse);
    }
  }
  
  public static final Column [] ALL 
    = Lists.newArrayList().toArray(new Column[0]);
  
  public static List<Column []> generateCuboids(Column [] columns) {
    int numCuboids = (int) Math.pow(2, columns.length);
    int maxBits = columns.length;    
    
    List<Column []> cube = Lists.newArrayList();
    List<Column> cuboidCols;
    
    cube.add(ALL);
    for (int cuboidId = 1; cuboidId < numCuboids; cuboidId++) {
      cuboidCols = Lists.newArrayList();
      for (int j = 0; j < maxBits; j++) {
        int bit = 1 << j;
        if ((cuboidId & bit) == bit) {
          cuboidCols.add(columns[j]);
        }
      }
      cube.add(cuboidCols.toArray(new Column[cuboidCols.size()]));
    }
    return cube;
  }

  private static LogicalNode createExplicitJoinTree(QueryBlock block) {
    return createExplicitJoinTree_(block.getJoinClause());
  }
  
  private static LogicalNode createExplicitJoinTree_(JoinClause joinClause) {

    JoinNode join;
    if (joinClause.hasLeftJoin()) {
      LogicalNode outer = createExplicitJoinTree_(joinClause.getLeftJoin());
      join = new JoinNode(joinClause.getJoinType(), outer);
      join.setInner(new ScanNode(joinClause.getRight()));
    } else {
      join = new JoinNode(joinClause.getJoinType(), new ScanNode(joinClause.getLeft()),
          new ScanNode(joinClause.getRight()));
    }
    if (joinClause.hasJoinQual()) {
      join.setJoinQual(joinClause.getJoinQual());
    } else if (joinClause.hasJoinColumns()) { 
      // for using clause of explicit join
      // TODO - to be implemented. Now, tajo only support 'ON' join clause.
    }
    
    // Determine Join Schemas
    Schema merged;
    if (joinClause.isNatural()) {
      merged = getNaturalJoin(join.getOuterNode(), join.getInnerNode());
    } else {
      merged = SchemaUtil.merge(join.getOuterNode().getOutSchema(),
          join.getInnerNode().getOutSchema());
    }
    
    join.setInSchema(merged);
    join.setOutSchema(merged);
    
    // Determine join quals
    // if natural join, should have the equi join conditions on common columns
    if (joinClause.isNatural()) {
      Schema leftSchema = join.getOuterNode().getOutSchema();
      Schema rightSchema = join.getInnerNode().getOutSchema();
      Schema commons = SchemaUtil.getCommons(
          leftSchema, rightSchema);
      EvalNode njCond = getNaturalJoinCondition(leftSchema, rightSchema, commons);
      join.setJoinQual(njCond);
    } else if (joinClause.hasJoinQual()) { 
      // otherwise, the given join conditions are set
      join.setJoinQual(joinClause.getJoinQual());
    }
    
    return join;
  }
  
  private static EvalNode getNaturalJoinCondition(Schema outer, Schema inner, Schema commons) {
    EvalNode njQual = null;
    EvalNode equiQual;
    
    Column leftJoinKey;
    Column rightJoinKey;
    for (Column common : commons.getColumns()) {
      leftJoinKey = outer.getColumnByName(common.getColumnName());
      rightJoinKey = inner.getColumnByName(common.getColumnName());
      equiQual = new BinaryEval(EvalNode.Type.EQUAL,
          new FieldEval(leftJoinKey), new FieldEval(rightJoinKey));
      if (njQual == null) {
        njQual = equiQual;
      } else {
        njQual = new BinaryEval(EvalNode.Type.AND,
            njQual, equiQual);
      }
    }
    
    return njQual;
  }
  
  private static LogicalNode createImplicitJoinTree(FromTable [] tables,
                                                    EvalNode [] cnf) {
    if (cnf == null) {
      return createCatasianProduct(tables);
    } else {
      return createCrossJoinFromJoinCondition(tables, cnf);
    }
  }

  private static LogicalNode createCrossJoinFromJoinCondition(
      FromTable [] tables, EvalNode [] cnf) {
    Map<String, FromTable> fromTableMap = Maps.newHashMap();
    for (FromTable f : tables) {
      // TODO - to consider alias and self-join
      fromTableMap.put(f.getTableName(), f);
    }

    JoinTree joinTree = new JoinTree(); // to infer join order
    for (EvalNode expr : cnf) {
      if (PlannerUtil.isJoinQual(expr)) {
        joinTree.addJoin(expr);
      }
    }

    List<String> remain = Lists.newArrayList(fromTableMap.keySet());
    remain.removeAll(joinTree.getTables()); // only remain joins not matched to any join condition
    List<Edge> joinOrder = null;
    LogicalNode subroot = null;
    JoinNode join;
    Schema joinSchema;

    // if there are at least one join matched to the one of join conditions,
    // we try to traverse the join tree in the depth-first manner and
    // determine the initial join order. Here, we do not consider the join cost.
    // The optimized join order will be considered in the optimizer.
    if (joinTree.getJoinNum() > 0) {
      Stack<String> stack = new Stack<String>();
      Set<String> visited = Sets.newHashSet();


      // initially, one table is pushed into the stack
      String seed = joinTree.getTables().iterator().next();
      stack.add(seed);

      joinOrder = Lists.newArrayList();

      while (!stack.empty()) {
        String table = stack.pop();
        if (visited.contains(table)) {
          continue;
        }
        visited.add(table);

        // 'joinOrder' will contain all tables corresponding to the given join conditions.
        for (Edge edge : joinTree.getEdges(table)) {
          if (!visited.contains(edge.getTarget()) && !edge.getTarget().equals(table)) {
            stack.add(edge.getTarget());
            joinOrder.add(edge);
          }
        }
      }

      subroot = new ScanNode(fromTableMap.get(joinOrder.get(0).getSrc()));
      LogicalNode inner;
      for (Edge edge : joinOrder) {
        inner = new ScanNode(fromTableMap.get(edge.getTarget()));
        join = new JoinNode(JoinType.CROSS_JOIN, subroot, inner);
        subroot = join;

        joinSchema = SchemaUtil.merge(
            join.getOuterNode().getOutSchema(),
            join.getInnerNode().getOutSchema());
        join.setInSchema(joinSchema);
        join.setOutSchema(joinSchema);
      }
    }

    // Here, there are two cases:
    // 1) there already exists the join plan.
    // 2) there are no join plan.
    if (joinOrder != null) { // case 1)
      // if there are join tables corresponding to any join condition,
      // the join plan is placed as the outer plan of the product.
      remain.remove(joinOrder.get(0).getSrc());
      remain.remove(joinOrder.get(0).getTarget());
    } else { // case 2)
      // if there are no inferred joins, the one of the remain join tables is placed as the left table
      subroot = new ScanNode(fromTableMap.get(remain.get(0)));
      remain.remove(remain.get(0));
    }

    // Here, the variable 'remain' contains join tables which are not matched to any join conditions.
    // Thus, they will be joined by catasian product
    for (String table : remain) {
      join = new JoinNode(JoinType.CROSS_JOIN,
          subroot, new ScanNode(fromTableMap.get(table)));
      joinSchema = SchemaUtil.merge(
          join.getOuterNode().getOutSchema(),
          join.getInnerNode().getOutSchema());
      join.setInSchema(joinSchema);
      join.setOutSchema(joinSchema);
      subroot = join;
    }

    return subroot;
  }

  // TODO - this method is somewhat duplicated to createCrossJoinFromJoinCondition. Later, it should be removed.
  private static LogicalNode createCatasianProduct(FromTable [] tables) {
    LogicalNode subroot = new ScanNode(tables[0]);
    Schema joinSchema;
    if(tables.length > 1) {
      for(int i=1; i < tables.length; i++) {
        JoinNode join = new JoinNode(JoinType.CROSS_JOIN,
            subroot, new ScanNode(tables[i]));
        joinSchema = SchemaUtil.merge(
            join.getOuterNode().getOutSchema(),
            join.getInnerNode().getOutSchema());
        join.setInSchema(joinSchema);
        join.setOutSchema(joinSchema);
        subroot = join;
      }
    }

    return subroot;
  }

  public static Schema getProjectedSchema(PlanningContext context, Target [] targets) {
    Schema projected = new Schema();
    for(Target t : targets) {
      DataType type = t.getEvalTree().getValueType()[0];
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else if (t.getEvalTree().getName().equals("?")) {
        name = context.getGeneratedColumnName();
      } else {
        name = t.getEvalTree().getName();
      }
      projected.addColumn(name,type);
    }

    return projected;
  }
  
  private static Schema getNaturalJoin(LogicalNode outer, LogicalNode inner) {
    Schema joinSchema = new Schema();
    Schema commons = SchemaUtil.getCommons(outer.getOutSchema(),
        inner.getOutSchema());
    joinSchema.addColumns(commons);
    for (Column c : outer.getOutSchema().getColumns()) {
      for (Column common : commons.getColumns()) {
        if (!common.getColumnName().equals(c.getColumnName())) {
          joinSchema.addColumn(c);
        }
      }
    }

    for (Column c : inner.getOutSchema().getColumns()) {
      for (Column common : commons.getColumns()) {
        if (!common.getColumnName().equals(c.getColumnName())) {
          joinSchema.addColumn(c);
        }
      }
    }
    return joinSchema;
  }
}