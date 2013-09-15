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
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.LogicalPlan.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.engine.utils.OuterJoinUtil.*;
import org.apache.tajo.engine.utils.OuterJoinUtil;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType; 
import org.apache.tajo.engine.eval.FieldEval;

import java.util.Stack;
import java.util.Collection;
import java.util.Iterator;

//camelia --
public class OuterJoinMetadata extends BasicLogicalPlanVisitor<Integer> {

   private static OuterJoinUtil oju2 = OuterJoinUtil.getOuterJoinUtil();
   private static final Log LOG = LogFactory.getLog(OuterJoinMetadata.class);
   private static String currentBlockName;

   public OuterJoinMetadata(LogicalPlan plan) throws PlanningException {
      //for each block in the plan, process its queryblock 
      Collection<QueryBlock> qBlocks = plan.getQueryBlocks();
      Iterator it = qBlocks.iterator();
      while(it.hasNext()) {
        QueryBlock qb = (QueryBlock) it.next();        
        LOG.info("******** QUERYBLOCK:" + qb.getName());

        //fill in the allTables for this qb
        Collection<ScanNode> relations = qb.getRelations();
        Iterator it2 = relations.iterator();

        while(it2.hasNext()) {
           ScanNode scan = (ScanNode) it2.next();
           LOG.info("******** TABLE:" + scan.getFromTable().getTableName());
           oju2.putTheTable(qb.getName(), scan.getFromTable());
        }

         //go visit nodes in this qb, to count
         Stack<LogicalNode> stack = new Stack<LogicalNode>();
         Integer depth = 0;
         currentBlockName = qb.getName();
         visitChild(plan, qb.getRoot(), stack, depth);

         LOG.info("TABLE    COUNTLEFT  COUNTRIGHT  COUNTFULL  COUNTINNER  ISNULLSUPPLYING   COUNTNULLSUPPLYING  ISNULLRESTRICTED DEPTHRESTRICTED\n");
         oju2.printAllTables(qb.getName());

      }

      
   }

   public LogicalNode visitJoin(LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack, Integer depth) throws PlanningException {
      LogicalNode left = joinNode.getLeftChild();
      LogicalNode right = joinNode.getRightChild();

      OuterJoinUtil.TableOuterJoined rightTable, leftTable;
      JoinType joinType = joinNode.getJoinType();
      boolean LeftIsLeft = false;  //because the order in the join condition may be however 
      String leftexprname;
      FieldEval fev = null;

      //ensure that the right child is a ScanNode, as in a left deep tree query plan. Otherwise raise exception
      //if (right.getClass().isInstance(ScanNode.class)==false)
      //   throw new PlanningException(" ERROR: The right child of the join node in left deep tree is not ScanNode");

      
      
      if (right.getClass().getName().equals("org.apache.tajo.engine.planner.logical.ScanNode")==false)      
         throw new PlanningException(" ERROR: The right child of the join node in left deep tree is not ScanNode, but " + right.getClass().getName());

      if (joinNode.hasJoinQual())
        if((joinNode.getJoinQual().getRightExpr().getType() == EvalType.FIELD)&&(joinNode.getJoinQual().getLeftExpr().getType() == EvalType.FIELD)){
         EvalNode joinQual = joinNode.getJoinQual();
         String rightname = ((FieldEval) joinQual.getRightExpr()).getTableId();

         if(((ScanNode)right). getTableId().equals(rightname) == true){
            LeftIsLeft = true;
            fev = (FieldEval) joinQual.getLeftExpr();
            leftexprname = fev.getTableId();
         }
         else{
            LeftIsLeft = false;
            fev = (FieldEval) joinQual.getRightExpr();
            leftexprname = fev.getTableId();
         }

         LOG.info("********* validate outer join: leftexprname=" + leftexprname + " while right_id=" + ((ScanNode)right). getTableId());


          if(joinType == JoinType.LEFT_OUTER){
	       //it is the right operand in a left outer join
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countLeft++;
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).isNullSupplying = true;
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countNullSupplying++;

	       //based on the join condition, put info for its left operand as well
	       oju2.getTheTable(currentBlockName, leftexprname).countLeft++;      
	  }
	  else if(joinType == JoinType.FULL_OUTER){
	       //it is the right operand in a full outer join
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countFull++;
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).isNullSupplying = true;
               oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countNullSupplying++;
	       
	       //based on the join condition, put info for its left operand as well
	       oju2.getTheTable(currentBlockName, leftexprname).countFull++;  
	       oju2.getTheTable(currentBlockName, leftexprname).isNullSupplying = true; 
               oju2.getTheTable(currentBlockName, leftexprname).countNullSupplying++;    
	  } 
	  else if(joinType == JoinType.RIGHT_OUTER){
	       //it is the right operand in a right outer join
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countRight++;

	       //based on the join condition, put info for its left operand as well
	       oju2.getTheTable(currentBlockName, leftexprname).countRight++;  
	       oju2.getTheTable(currentBlockName, leftexprname).isNullSupplying = true; 
	       oju2.getTheTable(currentBlockName, leftexprname).countNullSupplying++;
	  } 
	  else if(joinType == JoinType.INNER){
	       //it is the right operand in an inner join
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).countInner++;
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).isNullRestricted = true;
	       oju2.getTheTable(currentBlockName, ((ScanNode)right). getTableId()).depthRestricted = depth;

	       //based on the join condition, put info for its left operand as well
	       oju2.getTheTable(currentBlockName, leftexprname).countInner++;  
	       oju2.getTheTable(currentBlockName, leftexprname).isNullRestricted = true;
	       oju2.getTheTable(currentBlockName, leftexprname).depthRestricted = depth;
	       
	  } 


      }// join has qual


     
      visitChild(plan, left, stack, depth + 1);
      visitChild(plan, right, stack, depth +1);

      return joinNode;
   }

   private void recursiveWhere(EvalNode wherecond, int depth){
      LOG.info("******** WHERE (DEPTH:" + depth + ") : " + wherecond.getLeftExpr().toString() + " ["+wherecond.getType() + "] " + wherecond.getRightExpr().toString());
     
      if((wherecond.getLeftExpr().getType() == EvalType.FIELD) && (wherecond.getRightExpr().getType()== EvalType.FIELD)){
         //check if it is a inner join condition
         String lefttablename = ((FieldEval) wherecond.getLeftExpr()).getTableId();
         String righttablename = ((FieldEval) wherecond.getRightExpr()).getTableId();
         if(lefttablename.equals(righttablename) == false){
             //it is an inner join
             
             oju2.getTheTable(currentBlockName, lefttablename).countInner++;  
             oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
             oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
             oju2.getTheTable(currentBlockName, righttablename).countInner++;  
             oju2.getTheTable(currentBlockName, righttablename).isNullRestricted = true;
             oju2.getTheTable(currentBlockName, righttablename).depthRestricted = 0;
         }
         else {
             //it is a selection involving 2 columns of the same table
             oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
             oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
         }
     }
     else{
         //check if it a null-intolerant selection 
         //ON THE LEFT
         if((wherecond.getLeftExpr().getType() == EvalType.FIELD) && (wherecond.getType() == EvalType.IN)){
           String lefttablename= ((FieldEval) wherecond.getLeftExpr()).getTableId();
           oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
         }
         if((wherecond.getLeftExpr().getType() == EvalType.FIELD) && (wherecond.getType() == EvalType.LIKE)){
           String lefttablename= ((FieldEval) wherecond.getLeftExpr()).getTableId();
           oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
         }
         if((wherecond.getLeftExpr().getType() == EvalType.FIELD) && (wherecond.getType() != EvalType.IS_NULL)){
           String lefttablename= ((FieldEval) wherecond.getLeftExpr()).getTableId();
           oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
         }
         else if (wherecond.getLeftExpr().getType() == EvalType.FUNCTION){
           String lefttablename= ((FieldEval) wherecond.getLeftExpr()).getTableId();
           oju2.getTheTable(currentBlockName, lefttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, lefttablename).depthRestricted = 0;
         }
 
         //ON THE RIGHT
         //-- note: the (wherecond.getType()!=EvalNode.Type.IS) test is useless on the right case, it's always true
         if((wherecond.getRightExpr().getType() == EvalType.FIELD) && (wherecond.getType() == EvalType.IN)){
           String righttablename= ((FieldEval) wherecond.getRightExpr()).getTableId();
           oju2.getTheTable(currentBlockName, righttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, righttablename).depthRestricted = 0;
         }
         if((wherecond.getRightExpr().getType() == EvalType.FIELD) && (wherecond.getType() == EvalType.LIKE)){
           String righttablename= ((FieldEval) wherecond.getRightExpr()).getTableId();
           oju2.getTheTable(currentBlockName, righttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, righttablename).depthRestricted = 0;
         }
         if((wherecond.getRightExpr().getType() == EvalType.FIELD) && (wherecond.getType() != EvalType.IS_NULL)){
           String righttablename= ((FieldEval) wherecond.getRightExpr()).getTableId();
           oju2.getTheTable(currentBlockName, righttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, righttablename).depthRestricted = 0;
         }
         else if (wherecond.getRightExpr().getType() == EvalType.FUNCTION){
           String righttablename= ((FieldEval) wherecond.getRightExpr()).getTableId();
           oju2.getTheTable(currentBlockName, righttablename).isNullRestricted = true;
           oju2.getTheTable(currentBlockName, righttablename).depthRestricted = 0;
         }
     }

     if ((wherecond.getType() == EvalType.IN) || (wherecond.getType() == EvalType.LIKE))
        return;

     if((wherecond.getLeftExpr().getType() != EvalType.FIELD) && (wherecond.getLeftExpr().getType() != EvalType.FUNCTION) && (wherecond.getLeftExpr().getType() != EvalType.CONST) )
        recursiveWhere(wherecond.getLeftExpr(), depth+1);
     if((wherecond.getRightExpr().getType() != EvalType.FIELD) && (wherecond.getRightExpr().getType() != EvalType.FUNCTION) && (wherecond.getRightExpr().getType() != EvalType.CONST))
        recursiveWhere(wherecond.getRightExpr(), depth+1);
  }  



   public LogicalNode visitFilter(LogicalPlan plan, SelectionNode selNode, Stack<LogicalNode> stack, Integer depth)
      throws PlanningException {
      stack.push(selNode);
      //in the un-optimized form, the query block has only one selection node that contains exactly the compound condition in the WHERE clause of the (sub)query
      EvalNode wherecond = selNode.getQual();
      recursiveWhere(wherecond,0);   
      visitChild(plan, selNode.getChild(), stack, depth + 1);
      stack.pop();
      return selNode;
   }

   

}

//-- camelia
