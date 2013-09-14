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

public class OuterJoinRewriteRule extends BasicLogicalPlanVisitor<Integer> implements RewriteRule {
  private static final String NAME = "OuterJoinRewrite";
  private static OuterJoinUtil oju2 = OuterJoinUtil.getOuterJoinUtil();
  private static final Log LOG = LogFactory.getLog(OuterJoinRewriteRule.class);

  private static Stack<String> tablesStack=new Stack<String>();                      //stack for tables to be processed 
  private static Map<String,String> inStackTables = new HashMap<String,String>();    //hash map for tables to be processed 
  private static Map<String,String> processedTables = new HashMap<String,String>();  //hash map for already processed tables
  private static boolean isLastOuterJoin;
  private static String currentBlockName;
  private static String currentFunction; //is either RewriteMultiNullSupplier or RewriteNullRestricted

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    return PlannerUtil.findTopNode(toBeOptimized, NodeType.JOIN) != null;
  }

  public static void printStack(Map<String,String> inStackTables){
     String s="";
     for (String key : inStackTables.keySet()){
       s+=" | "+inStackTables.get(key).toString();
     }
     LOG.info("********* STACK :"+s+"\n");
  }


  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalNode root = plan.getRootBlock().getRoot();
    JoinNode joinNode = (JoinNode) PlannerUtil.findTopNode(root, NodeType.JOIN);

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    oju2 = OuterJoinUtil.getOuterJoinUtil();

    //for each queryBlock, check whether it contains at least one outer join
    //if so, perform rewriting within that query block 1) RewriteMultiNullSupplier and then 2)RewriteNullRestricted 
    for (String aBlock : oju2.allTables.keySet()){
       boolean hasOuter = false;
       for (String aTable : oju2.allTables.get(aBlock).keySet()){
          int cleft = oju2.allTables.get(aBlock).get(aTable).countLeft;
          int cright = oju2.allTables.get(aBlock).get(aTable).countRight;
          int cfull = oju2.allTables.get(aBlock).get(aTable).countFull;
          if((cleft + cright + cfull) >= 1){
             hasOuter = true;
          }
       }

       if(hasOuter){
          LOG.info("********** [QB " + aBlock + "] HAS OUTER JOIN => PROCEED TO REWRITING\n");
          String a;

          currentBlockName = aBlock;

          //1) firstly, rewriting takes place for situations where there are tables with countNullSupplying > 1; for these tables only the last join where it is null supplying has effect, the other unilateral outer joins where it is null supplying can be converted to inner joins. The rewriting will transform only unilateral outer joins (left/right) to inner joins, where needed. 
         //ensure that the tableStack is empty
         if(!tablesStack.empty()){
            LOG.info("ERROR: something wrong in outer join rewrite, step multi null supplier: stack is not empty");
            tablesStack.removeAllElements();
            inStackTables.clear();
         }
         currentFunction = "RewriteMultiNullSupplier";

         //fill in tablesStack
         for (String aTable : oju2.allTables.get(aBlock).keySet()){
             if(oju2.allTables.get(aBlock).get(aTable).countNullSupplying > 1){
                LOG.info("******** TABLE SUPPLYING NULLS IN MORE THAN ONE LEFT/RIGHT OUTER JOIN:" + oju2.allTables.get(aBlock).get(aTable).theTable.getTableName() + "\n");
                tablesStack.push(oju2.allTables.get(aBlock).get(aTable).theTable.getTableName());
                inStackTables.put(oju2.allTables.get(aBlock).get(aTable).theTable.getTableName(),oju2.allTables.get(aBlock).get(aTable).theTable.getTableName());       
            }
        }
        while(!tablesStack.empty()){
          //process the joins to retain only the rightmost join in a multi null supplier case
          printStack(inStackTables);
          isLastOuterJoin=true;

          visitChild(plan, plan.getBlock(aBlock).getRoot(), stack, 0); //start from the root LogicalNode of the aBlock at depth 0
          
          //eliminate top of tables stack
          a = tablesStack.pop();
          inStackTables.remove(a);  
        }



         //2) secondly, all null restricted tables are processes, to change, where appropriate, their outer joins to more restrictive join types
         //at this point tablesStack should be empty
         //ensure that the tableStack is empty
         if(!tablesStack.empty()){
            LOG.info("ERROR: something wrong in outer join rewrite, step null restricted: stack is not empty");
            tablesStack.removeAllElements();
            inStackTables.clear();
         }
         currentFunction = "RewriteNullRestricted";

         for (String aTable : oju2.allTables.get(aBlock).keySet()){
             if(oju2.allTables.get(aBlock).get(aTable).isNullRestricted == true){
                LOG.info("******** TABLE NULLRESTRICTED:" + oju2.allTables.get(aBlock).get(aTable).theTable.getTableName() + "\n");
                tablesStack.push(oju2.allTables.get(aBlock).get(aTable).theTable.getTableName());
                inStackTables.put(oju2.allTables.get(aBlock).get(aTable).theTable.getTableName(),oju2.allTables.get(aBlock).get(aTable).theTable.getTableName());       
            }
        }

         while(!tablesStack.empty()){
            printStack(inStackTables);
            //if this table is null supplier in at least one join
            a = tablesStack.peek();
            if(oju2.allTables.get(aBlock).get(a).countNullSupplying > 0){
               //isLastOuterJoin=true;
               LOG.info("this null restricted table is also null supplying");
               visitChild(plan, plan.getBlock(aBlock).getRoot(), stack, 0); //start from the root LogicalNode of the aBlock at depth 0
            }
         
            //eliminate top of stack and mark it as processed
            a = tablesStack.pop();
            inStackTables.remove(a);  
            processedTables.put(a, a);
        
           //new elements might have been pushed in the stack inside the recursive method call
         }//while    


          

       }//if hasouter

    }//for block
    
    
    return plan;
  }

  public LogicalNode visitJoin(LogicalPlan plan, JoinNode joinNode, Stack<LogicalNode> stack, Integer depth)
      throws PlanningException {
    LogicalNode left = joinNode.getLeftChild();
    LogicalNode right = joinNode.getRightChild();

    //if we are on a logical node from the currently analyzed query block(i.e. currentBlockName)
    LOG.info("CurrentBlockName= " + currentBlockName);
    //LOG.info("Current joinNode is in block = " + plan.getBlock(joinNode).getName());
    //if(plan.getBlock(joinNode).getName().equals(currentBlockName) == true)
    if(true){

       if(currentFunction.equals("RewriteMultiNullSupplier") == true) {
          String oneTable = tablesStack.peek(); //don't remove it until reaching all leaves
          boolean isLastOuterJoin_copy = isLastOuterJoin;

          if (joinNode.hasJoinQual())
             if((joinNode.getJoinQual().getRightExpr().getType() == EvalType.FIELD)&&(joinNode.getJoinQual().getLeftExpr().getType() == EvalType.FIELD)){
                String rightexprname = ((FieldEval) joinNode.getJoinQual().getRightExpr()).getTableId();
                String leftexprname = ((FieldEval) joinNode.getJoinQual().getLeftExpr()).getTableId();
                String otherTable = null;
                String b = null;
          
                JoinType joinType = joinNode.getJoinType();
                LOG.info("***** IN RewriteMultiNullSupplier join has type: " + joinType.toString() + " left node type:" + left.getType().toString() + "   and right node type:" + right.getType().toString()); 
                //if this join involves oneTable:
                if((rightexprname.equals(oneTable) == true) || (leftexprname.equals(oneTable) == true)){

                   // if it is a left outer join, and oneTable is null supplier 
                   if(joinType  == JoinType.LEFT_OUTER){
                      if(((ScanNode)right).getTableId().equals(oneTable) == true){
                         if(isLastOuterJoin == true){
                            LOG.info("***** IN RewriteMultiNullSupplier: WAS LAST NULL SUPPLYING JOIN FOR TABLE " + oneTable);
                            isLastOuterJoin_copy = false;
                         }
                         else {
                            //convert it to a inner join and add the other table as null restricted
                            LOG.info("***** IN RewriteMultiNullSupplier: WAS not LAST NULL SUPPLYING JOIN FOR TABLE " + oneTable + " => CHANGE IT TO INNER JOIN");
                            isLastOuterJoin_copy = false; //was false anyway
                            joinNode.setJoinType(JoinType.INNER);
                            oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;

                           if(leftexprname.equals(oneTable) == true){
                              oju2.getTheTable(currentBlockName, rightexprname).isNullRestricted = true;
                              oju2.getTheTable(currentBlockName, rightexprname).depthRestricted = depth;
                           }
                           else{
                              oju2.getTheTable(currentBlockName, leftexprname).isNullRestricted = true;
                              oju2.getTheTable(currentBlockName, leftexprname).depthRestricted = depth;
                           }

                        } 
                     } //scannode is oneTable
                    
                  }//left outer


                  // if it is a right outer join, and oneTable is null supplier 
                  if(joinType == JoinType.RIGHT_OUTER){
                    if(((ScanNode)right).getTableId().equals(oneTable) == false){
                       if(isLastOuterJoin == true){
                          LOG.info("***** IN RewriteMultiNullSupplier: WAS LAST NULL SUPPLYING JOIN FOR TABLE "+oneTable);
                          isLastOuterJoin_copy=false;
                       }
                       else {
                          //convert it to a inner join and add the other table as null restricted
                          joinNode.setJoinType(JoinType.INNER);
                          oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;
                          oju2.getTheTable(currentBlockName,((ScanNode)right).getTableId()).isNullRestricted = true;
                          oju2.getTheTable(currentBlockName,((ScanNode)right).getTableId()).depthRestricted = depth;
                       }
                    }                      
                 }//right outer

          }//this join involves oneTable

      } //hasJoinQual
      isLastOuterJoin = isLastOuterJoin_copy;
      
      visitChild(plan, left, stack, depth + 1);
      visitChild(plan, right, stack, depth + 1);


    }//function multinullsupplier
    else if(currentFunction.equals("RewriteNullRestricted") == true) {
       String oneTable = tablesStack.peek(); //don't remove it until reaching all leaves
       LOG.info("in visitChild in tablesStack, oneTable= " + oneTable);
       if (joinNode.hasJoinQual())
             if((joinNode.getJoinQual().getRightExpr().getType() == EvalType.FIELD)&&(joinNode.getJoinQual().getLeftExpr().getType() == EvalType.FIELD)){
                String rightexprname = ((FieldEval) joinNode.getJoinQual().getRightExpr()).getTableId();
                String leftexprname = ((FieldEval) joinNode.getJoinQual().getLeftExpr()).getTableId();
                String otherTable = null;
                String b = null;

                JoinType joinType = joinNode.getJoinType();
                LOG.info("***** IN RewriteRestrictedNullSupplier join has type: " + joinType.toString() + " left node type:" + left.getType().toString() + "   and right node type:" + right.getType().toString()); 
          
                
                // traverse logical plan tree  and for each join of type outer join where oneTable is null supplying, change the type of join accordingly. When the join is converted to inner join, the other operand table is pushed into the stack.
                LOG.info("oneTable=" + oneTable + " depthRestricted=" + oju2.getTheTable(currentBlockName, oneTable).depthRestricted + " in join:" + joinNode.getJoinQual() + "actual depth=" + depth);
         
                //if this join involves oneTable and it's at a level higher than the level where oneTable begins to be restricted:
                if(((rightexprname.equals(oneTable) == true)||(leftexprname.equals(oneTable) == true)) && (depth >= oju2.getTheTable(currentBlockName, oneTable).depthRestricted)){
                   LOG.info("at least one tableId is the oneTable");
                   if(((ScanNode)right).getTableId().equals(oneTable) == true){
                        LOG.info("oneTable is on the right");
                        /*oneTable is on the right side of this join. Cases are:   
                        1)  SELECT ... FROM   X  type JOIN oneTable ON  X.col1=oneTable.col2   
                        2)  SELECT ... FROM   X  type JOIN oneTable ON  oneTable.col2=X.col1  */  
                        // if jointype is  INNER or RIGHT_OUTER => no optimization 
                        // if jointype is FULL_OUTER => RIGHT_OUTER  [do not transform]
                        if(joinType == JoinType.LEFT_OUTER){
                           //this means that oneTable is a null supplier in this join => change join type to INNER ; decrement the countNullSUpplying in allTables for oneTable; if not already processed push the other operand in the stack
                           joinNode.setJoinType(JoinType.INNER);
                           oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;

                          //now get the other operand which becomes null restricted as well
                          if(rightexprname.equals(oneTable) == true) 
                             otherTable = leftexprname;
                          else
                             otherTable = rightexprname;

                          LOG.info("######### Transforming LEFT_OUTER to INNER join between " + oneTable + " and " + otherTable + "\n");
                           if((processedTables.get(otherTable) == null) && (inStackTables.get(otherTable) == null)){
                             b = tablesStack.pop();
                             tablesStack.push(otherTable);
                             tablesStack.push(b);
                             inStackTables.put(otherTable, otherTable);
                             LOG.info("====> pushed " + otherTable + " on stack \n");
                          }
                       
                      }//left outer  
                      else if(joinType == JoinType.FULL_OUTER){         
                          
                           joinNode.setJoinType(JoinType.RIGHT_OUTER);
                           oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;

                         
                          LOG.info("######### Transforming FULL_OUTER to RIGHT_OUTER join between " + oneTable + " and " + otherTable + "\n");                          


                      }//FULL OUTER          
                 }//oneTable is on the right
                 else {
                    /*oneTable is on the left side of this join. Cases are:   
                     1)  SELECT ... FROM   oneTable [... JOIN Y ON COND]  type JOIN X ON  X.col1=oneTable.col2   
                     2)  SELECT ... FROM   oneTable [... JOIN Y ON COND]  type JOIN X ON  oneTable.col2=X.col1
                     3)  SELECT ... FROM   Y ... JOIN oneTable ON COND    type JOIN X ON  X.col1=oneTable.col2
                     4)  SELECT ... FROM   Y ... JOIN oneTable ON COND    type JOIN X ON  oneTable.col2=X.col1*/  
                    //if jointype is INNER or LEFT_OUTER join => no optimization
                    // FULL_OUTER => LEFT_OUTER
                    LOG.info("oneTable is on the left");
                    if(joinType == JoinType.RIGHT_OUTER){
                       //this means that oneTable is a null supplier in this join =>  change join type to INNER ; decrement the countNullSUpplying in allTables for oneTable; if not already processed push the other operand in the stack
                       LOG.info("######### Transforming RIGHT_OUTER to INNER join between " + oneTable + " and " + ((ScanNode)right).getTableId() + "\n");
                       joinNode.setJoinType(JoinType.INNER);
                       oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;

                       //the other operand becomes null restricted as well
                       if((processedTables.get(((ScanNode)right).getTableId())==null)&&
                          (inStackTables.get(((ScanNode)right).getTableId())==null)){
                          b = tablesStack.pop();
                          tablesStack.push(((ScanNode)right).getTableId());
                          tablesStack.push(b);
                          inStackTables.put(((ScanNode)right).getTableId(),((ScanNode)right).getTableId());
                          LOG.info("====> pushed "+((ScanNode)right).getTableId()+" on stack \n");
                       }
             
                    }//right outer 
                    else if(joinType == JoinType.FULL_OUTER){  
                       joinNode.setJoinType(JoinType.LEFT_OUTER);
                       oju2.getTheTable(currentBlockName, oneTable).countNullSupplying--;

                         
                       LOG.info("######### Transforming FULL_OUTER to LEFT_OUTER join between " + oneTable + " and " + otherTable + "\n");                          


                    }// FULL OUTER
                  }//oneTable is on the left side      
         }//involves oneTable



       }//has joinqual

       //recursiveRewriteNullRestricted(outer, tablesStack, processedTables, inStackTables, depth + 1);//left-deep tree  
       visitChild(plan, left, stack, depth + 1);
       visitChild(plan, right, stack, depth + 1);

     }//if function RewriteNullRestricted

    }//if same block as currently analyzed one
    else {
       //if not on the currently analyzed block, just ignore and visit children
       visitChild(plan, left, stack, depth + 1);
       visitChild(plan, right, stack, depth + 1);
    } // else 

    return joinNode;
  }


}
// -- camelia
