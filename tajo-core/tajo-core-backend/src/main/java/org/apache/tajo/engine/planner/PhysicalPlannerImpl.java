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

/**
 *
 */
package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.IndexUtil;

import java.io.IOException;

public class PhysicalPlannerImpl implements PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlannerImpl.class);
  protected final TajoConf conf;
  protected final StorageManager sm;

  final long threshold = 1048576 * 128; // 64MB

  public PhysicalPlannerImpl(final TajoConf conf, final StorageManager sm) {
    this.conf = conf;
    this.sm = sm;
  }

  public PhysicalExec createPlan(final TaskAttemptContext context,
      final LogicalNode logicalPlan) throws InternalException {

    PhysicalExec plan;

    try {
      plan = createPlanRecursive(context, logicalPlan);

    } catch (IOException ioe) {
      throw new InternalException(ioe);
    }

    return plan;
  }

  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode) throws IOException {
    PhysicalExec outer;
    PhysicalExec inner;

    switch (logicalNode.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
        return createPlanRecursive(ctx, rootNode.getChild());

      case EXPRS:
        EvalExprNode evalExpr = (EvalExprNode) logicalNode;
        return new EvalExprExec(ctx, evalExpr);

      case STORE:
        StoreTableNode storeNode = (StoreTableNode) logicalNode;
        outer = createPlanRecursive(ctx, storeNode.getChild());
        return createStorePlan(ctx, storeNode, outer);

      case SELECTION:
        SelectionNode selNode = (SelectionNode) logicalNode;
        outer = createPlanRecursive(ctx, selNode.getChild());
        return new SelectionExec(ctx, selNode, outer);

      case PROJECTION:
        ProjectionNode prjNode = (ProjectionNode) logicalNode;
        outer = createPlanRecursive(ctx, prjNode.getChild());
        return new ProjectionExec(ctx, prjNode, outer);

      case SCAN:
        outer = createScanPlan(ctx, (ScanNode) logicalNode);
        return outer;

      case GROUP_BY:
        GroupbyNode grpNode = (GroupbyNode) logicalNode;
        outer = createPlanRecursive(ctx, grpNode.getChild());
        return createGroupByPlan(ctx, grpNode, outer);

      case SORT:
        SortNode sortNode = (SortNode) logicalNode;
        outer = createPlanRecursive(ctx, sortNode.getChild());
        return createSortPlan(ctx, sortNode, outer);

      case JOIN:
        JoinNode joinNode = (JoinNode) logicalNode;
        outer = createPlanRecursive(ctx, joinNode.getLeftChild());
        inner = createPlanRecursive(ctx, joinNode.getRightChild());
        return createJoinPlan(ctx, joinNode, outer, inner);

      case UNION:
        UnionNode unionNode = (UnionNode) logicalNode;
        outer = createPlanRecursive(ctx, unionNode.getLeftChild());
        inner = createPlanRecursive(ctx, unionNode.getRightChild());
        return new UnionExec(ctx, outer, inner);

      case LIMIT:
        LimitNode limitNode = (LimitNode) logicalNode;
        outer = createPlanRecursive(ctx, limitNode.getChild());
        return new LimitExec(ctx, limitNode.getInSchema(),
            limitNode.getOutSchema(), outer, limitNode);

      case BST_INDEX_SCAN:
        IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
        outer = createIndexScanExec(ctx, indexScanNode);
        return outer;

      default:
        return null;
    }
  }

  private long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) {
    long size = 0;
    for (String tableId : tableIds) {
      Fragment[] fragments = ctx.getTables(tableId);
      for (Fragment frag : fragments) {
        size += frag.getLength();
      }
    }
    return size;
  }

  public PhysicalExec createJoinPlan(TaskAttemptContext ctx, JoinNode joinNode,
                                     PhysicalExec outer, PhysicalExec inner)
      throws IOException {
    switch (joinNode.getJoinType()) {
      case CROSS:
        LOG.info("The planner chooses [Nested Loop Join]");
        return new NLJoinExec(ctx, joinNode, outer, inner);
      //camelia --
      case LEFT_OUTER:

         String [] innerLineage4 = PlannerUtil.getLineage(joinNode.getRightChild());
         long innerSize4 = estimateSizeRecursive(ctx, innerLineage4);
         
         if (innerSize4 < threshold) {
           // we can implement left outer join using hash join, using the right operand as the build relation
                    
           LOG.info("For left outer join ==> The planner chooses [modified Hash Join]");
           return new LeftOuter_HashJoinExec(ctx, joinNode, outer, inner);
         }
         else {
           //the right operand is too large, so we opt for NL implementation of left outer join
           LOG.info("For left outer join ==> The planner chooses [modified Nested Loop Join]");
           return new LeftOuter_NLJoinExec(ctx, joinNode, outer, inner);
         }

      case RIGHT_OUTER:

         //if the left operand is small enough => implement it as a left outer hash join with exchanged operators (note: blocking, but merge join is blocking as well)
         String [] outerLineage4 = PlannerUtil.getLineage(joinNode.getLeftChild());
         long outerSize4 = estimateSizeRecursive(ctx, outerLineage4);
         if (outerSize4 < threshold){
            LOG.info("For right outer join ==> The planner chooses [modified Hash Join]");
           return new LeftOuter_HashJoinExec(ctx, joinNode, inner, outer);

         }
         else {

            //the left operand is too large, so opt for merge join implementation
            LOG.info("For right outer join ==> The planner chooses [modified Merge Join]");
            SortSpec[][] sortSpecs2 = PlannerUtil.getSortKeysFromJoinQual(
               joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
            ExternalSortExec outerSort2 = new ExternalSortExec(ctx, sm,
               new SortNode(sortSpecs2[0], outer.getSchema(), outer.getSchema()),
               outer);
            ExternalSortExec innerSort2 = new ExternalSortExec(ctx, sm,
               new SortNode(sortSpecs2[1], inner.getSchema(), inner.getSchema()),
               inner);

            return new RightOuter_MergeJoinExec(ctx, joinNode, outerSort2, innerSort2,
               sortSpecs2[0], sortSpecs2[1]);
         }

      case FULL_OUTER:
         
         String [] outerLineage2 = PlannerUtil.getLineage(joinNode.getLeftChild());
         String [] innerLineage2 = PlannerUtil.getLineage(joinNode.getRightChild());
         long outerSize2 = estimateSizeRecursive(ctx, outerLineage2);
         long innerSize2 = estimateSizeRecursive(ctx, innerLineage2);

         final long threshold2 = 1048576 * 128; // 64MB

         boolean hashJoin2 = false;
         if (outerSize2 < threshold2 || innerSize2 < threshold2) {
           hashJoin2 = true;
         }

         if (hashJoin2) {
           PhysicalExec selectedOuter2;
           PhysicalExec selectedInner2;

           // HashJoinExec loads the inner relation to memory.
           if (outerSize2 <= innerSize2) {
             selectedInner2 = outer;
             selectedOuter2 = inner;
           } else {
             selectedInner2 = inner;
             selectedOuter2 = outer;
           }
           LOG.info("For full outer join ==> The planner chooses [modified Hash Join]");
           return new FullOuter_HashJoinExec(ctx, joinNode, selectedOuter2, selectedInner2);
         }
         else {
             //if size too large, full outer merge join implementation 
             LOG.info("For large full outer join ==> The planner chooses [modified Merge Join]");
             SortSpec[][] sortSpecs3 = PlannerUtil.getSortKeysFromJoinQual(
                joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
             ExternalSortExec outerSort3 = new ExternalSortExec(ctx, sm,
                new SortNode(sortSpecs3[0], outer.getSchema(), outer.getSchema()),
                outer);
             ExternalSortExec innerSort3 = new ExternalSortExec(ctx, sm,
                new SortNode(sortSpecs3[1], inner.getSchema(), inner.getSchema()),
                inner);

             return new FullOuter_MergeJoinExec(ctx, joinNode, outerSort3, innerSort3,
                sortSpecs3[0], sortSpecs3[1]);


         }

      //-- camelia

      case INNER:
        String [] outerLineage = PlannerUtil.getLineage(joinNode.getLeftChild());
        String [] innerLineage = PlannerUtil.getLineage(joinNode.getRightChild());
        long outerSize = estimateSizeRecursive(ctx, outerLineage);
        long innerSize = estimateSizeRecursive(ctx, innerLineage);

        final long threshold = 1048576 * 128; // 64MB

        boolean hashJoin = false;
        if (outerSize < threshold || innerSize < threshold) {
          hashJoin = true;
        }

        if (hashJoin) {
          PhysicalExec selectedOuter;
          PhysicalExec selectedInner;

          // HashJoinExec loads the inner relation to memory.
          if (outerSize <= innerSize) {
            selectedInner = outer;
            selectedOuter = inner;
          } else {
            selectedInner = inner;
            selectedOuter = outer;
          }

          LOG.info("The planner chooses [InMemory Hash Join]");
          return new HashJoinExec(ctx, joinNode, selectedOuter, selectedInner);
        }

      default:
        SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
            joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
        ExternalSortExec outerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[0], outer.getSchema(), outer.getSchema()),
            outer);
        ExternalSortExec innerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[1], inner.getSchema(), inner.getSchema()),
            inner);

        LOG.info("The planner chooses [Merge Join]");
        return new MergeJoinExec(ctx, joinNode, outerSort, innerSort,
            sortSpecs[0], sortSpecs[1]);
    }
  }

  public PhysicalExec createStorePlan(TaskAttemptContext ctx,
                                      StoreTableNode plan, PhysicalExec subOp) throws IOException {
    if (plan.hasPartitionKey()) {
      switch (plan.getPartitionType()) {
        case HASH:
          return new PartitionedStoreExec(ctx, sm, plan, subOp);

        case RANGE:
          SortSpec [] sortSpecs = null;
          if (subOp instanceof SortExec) {
            sortSpecs = ((SortExec)subOp).getSortSpecs();
          } else {
            Column[] columns = plan.getPartitionKeys();
            SortSpec specs[] = new SortSpec[columns.length];
            for (int i = 0; i < columns.length; i++) {
              specs[i] = new SortSpec(columns[i]);
            }
          }

          return new IndexedStoreExec(ctx, sm, subOp,
              plan.getInSchema(), plan.getInSchema(), sortSpecs);
      }
    }
    if (plan instanceof StoreIndexNode) {
      return new TunnelExec(ctx, plan.getOutSchema(), subOp);
    }

    return new StoreTableExec(ctx, sm, plan, subOp);
  }

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()),
        "Error: There is no table matched to %s", scanNode.getTableId());

    Fragment[] fragments = ctx.getTables(scanNode.getTableId());
    return new SeqScanExec(ctx, sm, scanNode, fragments);
  }

  public PhysicalExec createGroupByPlan(TaskAttemptContext ctx,
                                        GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    if (grpColumns.length == 0) {
      LOG.info("The planner chooses [Hash Aggregation]");
      return new HashAggregateExec(ctx, groupbyNode, subOp);
    } else {
      String [] outerLineage = PlannerUtil.getLineage(groupbyNode.getChild());
      long estimatedSize = estimateSizeRecursive(ctx, outerLineage);
      final long threshold = conf.getLongVar(TajoConf.ConfVars.HASH_AGGREGATION_THRESHOLD);

      // if the relation size is less than the threshold,
      // the hash aggregation will be used.
      if (estimatedSize <= threshold) {
        LOG.info("The planner chooses [Hash Aggregation]");
        return new HashAggregateExec(ctx, groupbyNode, subOp);
      } else {
        SortSpec[] specs = new SortSpec[grpColumns.length];
        for (int i = 0; i < grpColumns.length; i++) {
          specs[i] = new SortSpec(grpColumns[i], true, false);
        }
        SortNode sortNode = new SortNode(specs);
        sortNode.setInSchema(subOp.getSchema());
        sortNode.setOutSchema(subOp.getSchema());
        // SortExec sortExec = new SortExec(sortNode, child);
        ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode,
            subOp);
        LOG.info("The planner chooses [Sort Aggregation]");
        return new SortAggregateExec(ctx, groupbyNode, sortExec);
      }
    }
  }

  public PhysicalExec createSortPlan(TaskAttemptContext ctx, SortNode sortNode,
                                     PhysicalExec subOp) throws IOException {
    return new ExternalSortExec(ctx, sm, sortNode, subOp);
  }

  public PhysicalExec createIndexScanExec(TaskAttemptContext ctx,
                                          IndexScanNode annotation)
      throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getTableId()),
        "Error: There is no table matched to %s", annotation.getTableId());

    Fragment[] fragments = ctx.getTables(annotation.getTableId());

    String indexName = IndexUtil.getIndexNameOfFrag(fragments[0],
        annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableId()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(ctx, sm, annotation, fragments[0], new Path(
        indexPath, indexName), annotation.getKeySchema(), comp,
        annotation.getDatum());

  }
}
