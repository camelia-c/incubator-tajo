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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Stack;
import java.util.Map;
import java.util.HashMap;

import org.apache.tajo.engine.utils.OuterJoinUtil;

// camelia --
//this is a stub - working on...
public class OuterJoinRewriteRule extends BasicLogicalPlanVisitor<Integer> implements RewriteRule {
  private static final String NAME = "OuterJoinRewrite";
  private static OuterJoinUtil oju2 = OuterJoinUtil.getOuterJoinUtil();
  private static final Log LOG = LogFactory.getLog(OuterJoinRewriteRule.class);

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    return PlannerUtil.findTopNode(toBeOptimized, NodeType.JOIN) != null;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalNode root = plan.getRootBlock().getRoot();
    JoinNode joinNode = (JoinNode) PlannerUtil.findTopNode(root, NodeType.JOIN);

    Stack<LogicalNode> stack = new Stack<LogicalNode>();

    //check if there is at least one outer join in the whole plan
    boolean hasOuter = false;
    for (String key : oju2.allTables.keySet()){
       if(oju2.allTables.get(key).countLeft + oju2.allTables.get(key).countRight + oju2.allTables.get(key).countFull > 1){
          hasOuter = true;
       }
    }
    if(hasOuter){
       LOG.info("********** IT HAS OUTER JOIN => PROCEED TO REWRITING\n");
       Stack<String> tablesStack=new Stack<String>();                      //stack for tables to be processed 
       Map<String,String> inStackTables = new HashMap<String,String>();    //hash map for tables to be processed 
       Map<String,String> processedTables = new HashMap<String,String>();  //hash map for already processed tables
       String a;

       //firstly, rewriting takes place for situations where there are tables with countNullSupplying > 1; for these tables only the last join where it is null supplying has effect, the other unilateral outer joins where it is null supplying can be converted to inner joins. The rewriting will transform only unilateral outer joins (left/right) to inner joins, where needed. 
       


       //secondly, all null restricted tables are processes, to change, where appropriate, their outer joins to more restrictive join types
   


       visitChild(plan, root, stack, 10);
    }

    
    return plan;
  }

  public LogicalNode visitJoin(LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack, Integer x)
      throws PlanningException {
    LogicalNode left = joinNode.getRightChild();
    LogicalNode right = joinNode.getLeftChild();

    //TODO

    visitChild(plan, left, stack, x);
    visitChild(plan, right, stack, x);


    return joinNode;
  }


}
// -- camelia
