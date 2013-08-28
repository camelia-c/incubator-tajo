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

// camelia --
// for the moment, this is just a stub
public class OuterJoinRewriteRule extends BasicLogicalPlanVisitor<Integer> implements RewriteRule {
  private static final String NAME = "OuterJoinRewrite";

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

    visitChild(plan, root, stack, 10);
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
