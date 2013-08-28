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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.exception.InvalidQueryException;

import org.apache.tajo.engine.eval.EvalType; 
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.catalog.Column;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.Iterator;

import java.util.List;
import java.util.Stack;

public class FilterPushDownRule extends BasicLogicalPlanVisitor<List<EvalNode>> implements RewriteRule {
  private static final String NAME = "FilterPushDown";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    return PlannerUtil.findTopNode(toBeOptimized, NodeType.SELECTION) != null;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalNode root = plan.getRootBlock().getRoot();
    SelectionNode selNode = (SelectionNode) PlannerUtil.findTopNode(root, NodeType.SELECTION);
    Preconditions.checkNotNull(selNode);

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(selNode.getQual());

    visitChild(plan, root, stack, Lists.newArrayList(cnf));
    return plan;
  }

  public LogicalNode visitFilter(LogicalPlan plan, SelectionNode selNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {
    stack.push(selNode);
    visitChild(plan, selNode.getChild(), stack, cnf);
    stack.pop();

    // remove the selection operator if there is no search condition
    // after selection push.
    if(cnf.size() == 0) {
      LogicalNode node = stack.peek();
      if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) node;
        unary.setChild(selNode.getChild());
      } else {
        throw new InvalidQueryException("Unexpected Logical Query Plan");
      }
    }

    return selNode;
  }

  public LogicalNode visitJoin(LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    // camelia --
    //here we should stop selection pushdown on the null supplying side(s) of an outer join
    //get the two operands of the join operation as well as the join type
    JoinType jt = joinNode.getJoinType(); 
    if ((joinNode.hasJoinQual()==true)&&((jt==JoinType.LEFT_OUTER)||(jt==JoinType.RIGHT_OUTER)||(jt==JoinType.FULL_OUTER))){
       if((joinNode.getJoinQual().getLeftExpr().getType() == EvalType.FIELD)&&(joinNode.getJoinQual().getRightExpr().getType() == EvalType.FIELD)){
          String leftexprname = ((FieldEval) joinNode.getJoinQual().getLeftExpr()).getTableId();
          String rightexprname = ((FieldEval) joinNode.getJoinQual().getRightExpr()).getTableId();
          List<String> nullSuppliers = Lists.newArrayList();
          String [] outerlineage = PlannerUtil.getLineage(joinNode.getLeftChild());
          String [] innerlineage = PlannerUtil.getLineage(joinNode.getRightChild());
          Set<String> o = Sets.newHashSet(outerlineage);
          Set<String> i = Sets.newHashSet(innerlineage);

          //some verification
          if(jt==JoinType.FULL_OUTER){
             nullSuppliers.add(leftexprname);
             nullSuppliers.add(rightexprname);
             //verify that these null suppliers are indeed in the o and i sets
             if((i.contains(nullSuppliers.get(0))==false)&&(o.contains(nullSuppliers.get(0))==false))
                throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
             if((i.contains(nullSuppliers.get(1))==false)&&(o.contains(nullSuppliers.get(1))==false))
                throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");          

          }
          else if (jt==JoinType.LEFT_OUTER){
             nullSuppliers.add(((ScanNode)joinNode.getRightChild()).getTableId()); 
             //verify that this null supplier is indeed in the right sub-tree 
             if (i.contains(nullSuppliers.get(0))==false)
                 throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
            
          }
          else if (jt==JoinType.RIGHT_OUTER){
             if (((ScanNode)joinNode.getRightChild()).getTableId().equals(rightexprname)==true) {
                 nullSuppliers.add(leftexprname);
          }
          else {
             nullSuppliers.add(rightexprname);                              
          }
          //verify that this null supplier is indeed in the left sub-tree
          if (o.contains(nullSuppliers.get(0))==false)
             throw new InvalidQueryException("Incorrect Logical Query Plan with regard to outer join");
         }
         
         //retain in this outer join node's JoinQual those selection predicates related to the outer join's null supplier(s)
          
         List<EvalNode> matched2 = Lists.newArrayList();
         for (EvalNode eval : cnf) {
            
            Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);
            Set<String> tableIds = Sets.newHashSet();
            // getting distinct table references
            for (Column col : columnRefs) {
              if (!tableIds.contains(col.getTableName())) {
                tableIds.add(col.getTableName());
              }
            }
            
            //if the predicate involves any of the null suppliers
            boolean shouldKeep=false;
            Iterator<String> it2 = nullSuppliers.iterator();
            while(it2.hasNext()){
              if(tableIds.contains(it2.next()) == true) {
                   shouldKeep = true; 
              }
            }

            if(shouldKeep == true) {
                matched2.add(eval);
            }
            
          }

          //merge the retained predicates and establish them in the current outer join node. Then remove them from the cnf
          EvalNode qual2 = null;
          if (matched2.size() > 1) {
             // merged into one eval tree
             qual2 = EvalTreeUtil.transformCNF2Singleton(
                        matched2.toArray(new EvalNode [matched2.size()]));
          } else if (matched2.size() == 1) {
             // if the number of matched expr is one
             qual2 = matched2.get(0);
          }
          if (qual2 != null){
             EvalNode conjQual2 = EvalTreeUtil.transformCNF2Singleton(joinNode.getJoinQual(), qual2);
             joinNode.setJoinQual(conjQual2);
             cnf.removeAll(matched2);
          }
          
         //for the remaining cnf, push it as usual
       }
    }
    // -- camelia

    visitChild(plan, left, stack, cnf);
    visitChild(plan, right, stack, cnf);

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, joinNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = EvalTreeUtil.transformCNF2Singleton(
          matched.toArray(new EvalNode[matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) {
      if (joinNode.hasJoinQual()) {
        EvalNode conjQual = EvalTreeUtil.
            transformCNF2Singleton(joinNode.getJoinQual(), qual);
        joinNode.setJoinQual(conjQual);
      } else {
        joinNode.setJoinQual(qual);
      }
      if (joinNode.getJoinType() == JoinType.CROSS_JOIN) {
        joinNode.setJoinType(JoinType.INNER);
      }
      cnf.removeAll(matched);
    }

    return joinNode;
  }

  public LogicalNode visitScan(LogicalPlan plan, ScanNode scanNode, Stack<LogicalNode> stack, List<EvalNode> cnf)
      throws PlanningException {

    List<EvalNode> matched = Lists.newArrayList();
    for (EvalNode eval : cnf) {
      if (PlannerUtil.canBeEvaluated(eval, scanNode)) {
        matched.add(eval);
      }
    }

    EvalNode qual = null;
    if (matched.size() > 1) {
      // merged into one eval tree
      qual = EvalTreeUtil.transformCNF2Singleton(
          matched.toArray(new EvalNode [matched.size()]));
    } else if (matched.size() == 1) {
      // if the number of matched expr is one
      qual = matched.get(0);
    }

    if (qual != null) { // if a matched qual exists
      scanNode.setQual(qual);
    }

    cnf.removeAll(matched);

    return scanNode;
  }
}
