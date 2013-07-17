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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.parser.CreateTableStmt;
import org.apache.tajo.engine.parser.ParseTree;
import org.apache.tajo.engine.parser.QueryBlock;
import org.apache.tajo.engine.parser.QueryBlock.Target;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.query.exception.InvalidQueryException;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.util.IndexUtil;

import java.io.IOException;
import java.util.*;

import org.apache.tajo.engine.utils.OuterJoinUtil;

/**
 * This class optimizes a logical plan corresponding to one query block.
 */
public class LogicalOptimizer {
  private static Log LOG = LogFactory.getLog(LogicalOptimizer.class);
  
  private LogicalOptimizer() {
  }

  //camelia ---(
  private static OuterJoinUtil oju2 = OuterJoinUtil.getOuterJoinUtil();

  public static void printStack(Map<String,String> inStackTables){
     String s="";
     for (String key : inStackTables.keySet()){
       s+=" | "+inStackTables.get(key).toString();
     }
     LOG.info("********* STACK :"+s+"\n");
  }


  private static void recursiveRewriteMultiNullSupplier(LogicalNode plan, Stack<String> tablesStack, boolean isLastOuterJoin,int depth){
     //if this is a multi null supplier, then in the joinclause, all other joins but the last it participates in, change to inner join
     /* e.g. 1)   X left outer join Y on X.x=Y.y right outer join Z on Y.w=Z.z <=>  
                  X join Y on X.x=Y.y right outer join Z on Y.w=Z.z 
             2)   A right outer join B on A.a=B.b right outer join C on A.d=C.c <=> 
                  A join B on A.a=B.b right outer join C on A.d=C.c 
    */
    
    String oneTable = tablesStack.peek(); //don't remove it until reaching all leaves
    boolean isLastOuterJoin_copy = isLastOuterJoin;
    switch(plan.getType()) {
    
    case JOIN:
      JoinNode join = (JoinNode) plan;

      LogicalNode outer = join.getOuterNode();
      LogicalNode inner = join.getInnerNode();

      if (join.hasJoinQual()){
          String rightexprname = ((FieldEval) join.getJoinQual().getRightExpr()).getTableId();
          String leftexprname = ((FieldEval) join.getJoinQual().getLeftExpr()).getTableId();
          String otherTable = null;
          String b = null;
          
          JoinType jt = join.getJoinType();
          LOG.info("***** IN recursiveRewriteMultiNullSupplier join has type: " + jt.toString() + " inner exprtype:" + inner.getType() + "   and outer exprtype:" + outer.getType()); 
          //if this join involves oneTable:
          if((rightexprname.equals(oneTable) == true) || (leftexprname.equals(oneTable) == true)){

             // if it is a left outer join, and oneTable is null supplier 
             if(jt == JoinType.LEFT_OUTER){
                if(((ScanNode)inner).getTableId().equals(oneTable) == true){
                   if(isLastOuterJoin == true){
                      LOG.info("***** IN recursiveRewriteMultiNullSupplier: WAS LAST NULL SUPPLYING JOIN FOR TABLE "+oneTable);
                      isLastOuterJoin_copy = false;
                   }
                   else {
                      //convert it to a inner join and add the other table as null restricted
                      LOG.info("***** IN recursiveRewriteMultiNullSupplier: WAS not LAST NULL SUPPLYING JOIN FOR TABLE " + oneTable + " => CHANGE IT TO INNER JOIN");
                      isLastOuterJoin_copy = false; //was false anyway
                      join.setJoinType(JoinType.INNER);
                      oju2.allTables.get(oneTable).countNullSupplying--;

                      if(leftexprname.equals(oneTable) == true){
                          oju2.allTables.get(rightexprname).isNullRestricted = true;
                          oju2.allTables.get(rightexprname).depthRestricted = depth;
                      }
                      else{
                          oju2.allTables.get(leftexprname).isNullRestricted = true;
                          oju2.allTables.get(leftexprname).depthRestricted = depth;
                      }

                   } 
                } 
                    
              }//left outer


              // if it is a right outer join, and oneTable is null supplier 
              if(jt == JoinType.RIGHT_OUTER){
                 if(((ScanNode)inner).getTableId().equals(oneTable) == false){
                    if(isLastOuterJoin == true){
                       LOG.info("***** IN recursiveRewriteMultiNullSupplier: WAS LAST NULL SUPPLYING JOIN FOR TABLE "+oneTable);
                       isLastOuterJoin_copy=false;
                    }
                    else {
                       //convert it to a inner join and add the other table as null restricted
                       join.setJoinType(JoinType.INNER);
                       oju2.allTables.get(oneTable).countNullSupplying--;
                       oju2.allTables.get(((ScanNode)inner).getTableId()).isNullRestricted = true;
                       oju2.allTables.get(((ScanNode)inner).getTableId()).depthRestricted = depth;
                    }
                 }                      
              }//right outer

          }//this join involves oneTable

      } //hasJoinQual

      recursiveRewriteMultiNullSupplier(outer,tablesStack,isLastOuterJoin_copy,depth+1); //left-deep tree
      
      break;

    case SCAN:
      return;

    default:

      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        recursiveRewriteMultiNullSupplier(unary.getSubNode(),tablesStack,isLastOuterJoin_copy,depth+1);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        recursiveRewriteMultiNullSupplier(binary.getOuterNode(),tablesStack,isLastOuterJoin_copy,depth+1);
        recursiveRewriteMultiNullSupplier(binary.getInnerNode(),tablesStack,isLastOuterJoin_copy,depth+1);
        }

      break;
    }//switch

  }


   private static void recursiveRewriteNullRestricted(LogicalNode plan, Stack<String> tablesStack, Map<String,String> processedTables, Map<String,String> inStackTables, int depth){

    String oneTable = tablesStack.peek(); //don't remove it until reaching all leaves

    switch(plan.getType()) {
    
    case JOIN:
      JoinNode join = (JoinNode) plan;

      LogicalNode outer = join.getOuterNode();
      LogicalNode inner = join.getInnerNode();

      if (join.hasJoinQual()){
         JoinType jt = join.getJoinType();
         LOG.info("***** IN recursiveRewriteMultiNullSupplier join has type: " + jt.toString() + " inner exprtype:" + inner.getType() + "   and outer exprtype:" + outer.getType()); 
         String rightexprname = ((FieldEval) join.getJoinQual().getRightExpr()).getTableId();
         String leftexprname = ((FieldEval) join.getJoinQual().getLeftExpr()).getTableId();
         String otherTable = null;
         String b=null;

        // traverse logical plan tree  and for each join of type outer join where oneTable is null supplying, change the type of join accordingly. When the join is converted to inner join, the other operand table is pushed into the stack.
         LOG.info("oneTable=" + oneTable + " depthRestricted=" + oju2.allTables.get(oneTable).depthRestricted + " in join:" + join.getJoinQual() + "actual depth=" + depth);
         
         //if this join involves oneTable and it's at a level higher than the level where oneTable begins to be restricted:
         if(((rightexprname.equals(oneTable) == true)||(leftexprname.equals(oneTable) == true)) && (depth >= oju2.allTables.get(oneTable).depthRestricted)){
         
                   if(((ScanNode)inner).getTableId().equals(oneTable) == true){
                        /*oneTable is on the right side of this join. Cases are:   
                        1)  SELECT ... FROM   X  type JOIN oneTable ON  X.col1=oneTable.col2   
                        2)  SELECT ... FROM   X  type JOIN oneTable ON  oneTable.col2=X.col1  */  
                        // if jointype is  INNER or RIGHT_OUTER => no optimization 
                        // if jointype is FULL_OUTER => do not transform
                        if(jt == JoinType.LEFT_OUTER){
                           //this means that oneTable is a null supplier in this join => change join type to INNER ; decrement the countNullSUpplying in allTables for oneTable; if not already processed push the other operand in the stack
                           join.setJoinType(JoinType.INNER);
                           oju2.allTables.get(oneTable).countNullSupplying--;

                          //now get the other operand which becomes null restricted as well
                          if(rightexprname.equals(oneTable) == true) 
                             otherTable = leftexprname;
                          else
                             otherTable = rightexprname;

                          LOG.info("######### Transforming LEFT_OUTER to INNER join between " + oneTable + " and " + otherTable + "\n");
                           if((processedTables.get(otherTable) == null) && (inStackTables.get(otherTable) == null)){
                             b=tablesStack.pop();
                             tablesStack.push(otherTable);
                             tablesStack.push(b);
                             inStackTables.put(otherTable, otherTable);
                             LOG.info("====> pushed " + otherTable + " on stack \n");
                          }
                       
                      }//left outer                     
                 }//oneTable is on the right
                 else {
                    /*oneTable is on the left side of this join. Cases are:   
                     1)  SELECT ... FROM   oneTable [... JOIN Y ON COND]  type JOIN X ON  X.col1=oneTable.col2   
                     2)  SELECT ... FROM   oneTable [... JOIN Y ON COND]  type JOIN X ON  oneTable.col2=X.col1
                     3)  SELECT ... FROM   Y ... JOIN oneTable ON COND    type JOIN X ON  X.col1=oneTable.col2
                     4)  SELECT ... FROM   Y ... JOIN oneTable ON COND    type JOIN X ON  oneTable.col2=X.col1*/  
                    //if jointype is INNER or LEFT_OUTER join => no optimization
                    if(jt == JoinType.RIGHT_OUTER){
                       //this means that oneTable is a null supplier in this join =>  change join type to INNER ; decrement the countNullSUpplying in allTables for oneTable; if not already processed push the other operand in the stack
                       LOG.info("######### Transforming RIGHT_OUTER to INNER join between " + oneTable + " and " + ((ScanNode)inner).getTableId() + "\n");
                       join.setJoinType(JoinType.INNER);
                       oju2.allTables.get(oneTable).countNullSupplying--;

                       //the other operand becomes null restricted as well
                       if((processedTables.get(((ScanNode)inner).getTableId())==null)&&
                          (inStackTables.get(((ScanNode)inner).getTableId())==null)){
                          b=tablesStack.pop();
                          tablesStack.push(((ScanNode)inner).getTableId());
                          tablesStack.push(b);
                          inStackTables.put(((ScanNode)inner).getTableId(),((ScanNode)inner).getTableId());
                          LOG.info("====> pushed "+((ScanNode)inner).getTableId()+" on stack \n");
                       }
             
                    }//right outer  
                  }//oneTable is on the left side      
         }//involves oneTable
      }//hasJoinQual

      recursiveRewriteNullRestricted(outer, tablesStack, processedTables, inStackTables, depth + 1);//left-deep tree  
      
      break;

    case SCAN:
      return;

    default:

      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        recursiveRewriteNullRestricted(unary.getSubNode(), tablesStack, processedTables, inStackTables, depth+1);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        recursiveRewriteNullRestricted(binary.getOuterNode(), tablesStack, processedTables, inStackTables, depth+1);
        recursiveRewriteNullRestricted(binary.getInnerNode(), tablesStack, processedTables, inStackTables, depth+1);
        }

      break;
    }//switch

  }

  private static void rewriteOuterJoin(PlanningContext ctx, LogicalNode plan) {
    
    Stack<String> tablesStack=new Stack<String>();                      //stack for tables to be processed 
    Map<String,String> inStackTables = new HashMap<String,String>();    //hash map for tables to be processed 
    Map<String,String> processedTables = new HashMap<String,String>();  //hash map for already processed tables
    String a;
   
    //LOG.info("********** IN REWRITE OUTER JOIN\n");
    //oju2.printAllTables();

    //firstly, rewriting takes place for situations where there are tables with countNullSupplying > 1; for these tables only the last join where it is null supplying has effect, the other unilateral outer joins where it is null supplying can be converted to inner joins. The rewriting will transform only unilateral outer joins (left/right) to inner joins, where needed. 
    for (String key : oju2.allTables.keySet()){
         if(oju2.allTables.get(key).countNullSupplying > 1){
            LOG.info("******** TABLE SUPPLYING NULLS IN MORE THAN ONE LEFT/RIGHT OUTER JOIN:" + oju2.allTables.get(key).theTable.getTableName() + "\n");
            tablesStack.push(oju2.allTables.get(key).theTable.getTableName());
            inStackTables.put(oju2.allTables.get(key).theTable.getTableName(),oju2.allTables.get(key).theTable.getTableName());       
         }
     }
     while(!tablesStack.empty()){
         //process the joins to retain only the rightmost join in a multi null supplier case
         printStack(inStackTables);
         recursiveRewriteMultiNullSupplier(plan,tablesStack, true, 0);
    
         //eliminate top of tables stack
         a = tablesStack.pop();
         inStackTables.remove(a);  
     }

     //secondly, all null restricted tables are processes, to change, where appropriate, their outer joins to more restrictive join types
    for (String key : oju2.allTables.keySet()){
        if(oju2.allTables.get(key).isNullRestricted == true){           
            LOG.info("******** TABLE NULLRESTRICTED:" + oju2.allTables.get(key).theTable.getTableName() + "\n");            
            tablesStack.push(oju2.allTables.get(key).theTable.getTableName());
            inStackTables.put(oju2.allTables.get(key).theTable.getTableName(),oju2.allTables.get(key).theTable.getTableName());       
         }
    }
    while(!tablesStack.empty()){
         printStack(inStackTables);
         //if this table is null supplier in at least one join
         a = tablesStack.peek();
         if(oju2.allTables.get(a).countNullSupplying > 0){
             recursiveRewriteNullRestricted(plan, tablesStack, processedTables, inStackTables, 0);
         }
         
         //eliminate top of stack and mark it as processed
         a = tablesStack.pop();
         inStackTables.remove(a);  
         processedTables.put(a, a);
        
         //new elements might have been pushed in the stack inside the recursive method call
       }
  }
  //camelia )---

  public static LogicalNode optimize(PlanningContext context, LogicalNode plan) {
    LogicalNode toBeOptimized;
    boolean hasOuter=false;

    try {
      toBeOptimized = (LogicalNode) plan.clone();
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
      throw new InvalidQueryException("Cannot clone: " + plan);
    }

    switch (context.getParseTree().getType()) {
      case SELECT:
        //case UNION: // TODO - to be implemented
        //case EXCEPT:
        //case INTERSECT:
      case CREATE_TABLE_AS:
        //camelia ---(
        // if there is at least one outer join, go for rewriting
        for (String key : oju2.allTables.keySet()){
            if(oju2.allTables.get(key).countLeft + oju2.allTables.get(key).countRight + oju2.allTables.get(key).countFull > 1)
                hasOuter = true;
        }
        if(hasOuter){
          LOG.info("********** IT HAS OUTER JOIN => PROCEED TO REWRITING\n");
          rewriteOuterJoin(context, toBeOptimized);
        }
        //camelia )---

        // if there are selection node
        if(PlannerUtil.findTopNode(plan, ExprType.SELECTION) != null) {
          pushSelection(context, toBeOptimized);
        }

        try {
          pushProjection(context, toBeOptimized);
        } catch (CloneNotSupportedException e) {
          throw new InvalidQueryException(e);
        }

        break;
      default:
    }

    return toBeOptimized;
  }
  
  public static LogicalNode pushIndex(LogicalNode plan , StorageManager sm) throws IOException {
    if(PlannerUtil.findTopNode(plan, ExprType.SCAN) == null) {
      return plan;
    }
    LogicalNode toBeOptimized;
    try {
      toBeOptimized = (LogicalNode) plan.clone();
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
      throw new InvalidQueryException("Cannot clone: " + plan);
    }
  
    changeScanToIndexNode(null ,toBeOptimized , sm);
    return toBeOptimized;
    
  }
  private static void changeScanToIndexNode
    (LogicalNode parent , LogicalNode cur , StorageManager sm ) throws IOException {
    if( cur instanceof BinaryNode) {
      changeScanToIndexNode(cur , ((BinaryNode)cur).getOuterNode() , sm);
      changeScanToIndexNode(cur , ((BinaryNode)cur).getInnerNode() , sm);
    } else {
      switch(cur.getType()) {
      case CREATE_INDEX:
        return;
      case SCAN:
        ScanNode scan = (ScanNode)cur;
        EvalNode qual = scan.getQual();
        if(qual == null) { 
          return;
        }else {
          String tableName = scan.getTableId();
          Path path = new Path(sm.getTablePath(tableName), "index");
         
          if(sm.getFileSystem().exists(path)) {
            
            TableMeta meta = sm.getTableMeta(path);       
            IndexScanNode node;
            if ((node = IndexUtil.indexEval(scan, meta.getOptions())) == null) {
              return;
            }
            if( parent instanceof BinaryNode ) {
              if (scan.equals(((BinaryNode)parent).getOuterNode())) {
                ((BinaryNode)parent).setOuter(node);
              } else {
                ((BinaryNode)parent).setInner(node);
               }
            } else {
              ((UnaryNode)parent).setSubNode(node);
            }
          }
          return;
        }
      default:
        changeScanToIndexNode(cur , ((UnaryNode)cur).getSubNode() , sm);
        break;
      }
    }
  }
  /**
   * This method pushes down the selection into the appropriate sub 
   * logical operators.
   * <br />
   * 
   * There are three operators that can have search conditions.
   * Selection, Join, and GroupBy clause.
   * However, the search conditions of Join and GroupBy cannot be pushed down 
   * into child operators because they can be used when the data layout change
   * caused by join and grouping relations.
   * <br />
   * 
   * However, some of the search conditions of selection clause can be pushed 
   * down into appropriate sub operators. Some comparison expressions on 
   * multiple relations are actually join conditions, and other expression 
   * on single relation can be used in a scan operator or an Index Scan 
   * operator.   
   * 
   * @param ctx
   * @param plan
   */
  private static void pushSelection(PlanningContext ctx, LogicalNode plan) {
    SelectionNode selNode = (SelectionNode) PlannerUtil.findTopNode(plan,
        ExprType.SELECTION);
    Preconditions.checkNotNull(selNode);
    
    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(selNode.getQual());
    pushSelectionRecursive(plan, Lists.newArrayList(cnf), stack);
  }

  private static void pushSelectionRecursive(LogicalNode plan,
                                             List<EvalNode> cnf, Stack<LogicalNode> stack) {
    
    switch(plan.getType()) {
    
    case SELECTION:
      SelectionNode selNode = (SelectionNode) plan;
      stack.push(selNode);
      pushSelectionRecursive(selNode.getSubNode(),cnf, stack);
      stack.pop();
      
      // remove the selection operator if there is no search condition 
      // after selection push.
      if(cnf.size() == 0) {
        LogicalNode node = stack.peek();
        if (node instanceof UnaryNode) {
          UnaryNode unary = (UnaryNode) node;
          unary.setSubNode(selNode.getSubNode());
        } else {
          throw new InvalidQueryException("Unexpected Logical Query Plan");
        }
      }
      break;
    case JOIN:
      JoinNode join = (JoinNode) plan;

      LogicalNode outer = join.getOuterNode();
      LogicalNode inner = join.getInnerNode();

      pushSelectionRecursive(outer, cnf, stack);
      pushSelectionRecursive(inner, cnf, stack);

      List<EvalNode> matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (canBeEvaluated(eval, plan)) {
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

      if (qual != null) {
        JoinNode joinNode = (JoinNode) plan;
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

      break;

    case SCAN:
      matched = Lists.newArrayList();
      for (EvalNode eval : cnf) {
        if (canBeEvaluated(eval, plan)) {
          matched.add(eval);
        }
      }

      qual = null;
      if (matched.size() > 1) {
        // merged into one eval tree
        qual = EvalTreeUtil.transformCNF2Singleton(
            matched.toArray(new EvalNode [matched.size()]));
      } else if (matched.size() == 1) {
        // if the number of matched expr is one
        qual = matched.get(0);
      }

      if (qual != null) { // if a matched qual exists
        ScanNode scanNode = (ScanNode) plan;
        scanNode.setQual(qual);
      }

      cnf.removeAll(matched);
      break;

    default:
      stack.push(plan);
      if (plan instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode) plan;
        pushSelectionRecursive(unary.getSubNode(), cnf, stack);
      } else if (plan instanceof BinaryNode) {
        BinaryNode binary = (BinaryNode) plan;
        pushSelectionRecursive(binary.getOuterNode(), cnf, stack);
        pushSelectionRecursive(binary.getInnerNode(), cnf, stack);
      }
      stack.pop();
      break;
    }
  }
  
  public static boolean canBeEvaluated(EvalNode eval, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(eval);

    if (node.getType() == ExprType.JOIN) {
      JoinNode joinNode = (JoinNode) node;
      Set<String> tableIds = Sets.newHashSet();
      // getting distinct table references
      for (Column col : columnRefs) {
        if (!tableIds.contains(col.getTableName())) {
          tableIds.add(col.getTableName());
        }
      }
      
      // if the references only indicate two relation, the condition can be 
      // pushed into a join operator.
      if (tableIds.size() != 2) {
        return false;
      }
      
      String [] outer = PlannerUtil.getLineage(joinNode.getOuterNode());
      String [] inner = PlannerUtil.getLineage(joinNode.getInnerNode());

      Set<String> o = Sets.newHashSet(outer);
      Set<String> i = Sets.newHashSet(inner);
      if (outer == null || inner == null) {      
        throw new InvalidQueryException("ERROR: Unexpected logical plan");
      }
      Iterator<String> it = tableIds.iterator();
      if (o.contains(it.next()) && i.contains(it.next())) {
        return true;
      }
      
      it = tableIds.iterator();
      if (i.contains(it.next()) && o.contains(it.next())) {
        return true;
      }
      
      return false;
    } else {
      for (Column col : columnRefs) {
        if (!node.getInSchema().contains(col.getQualifiedName())) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * This method pushes down the projection list into the appropriate and
   * below logical operators.
   * @param context
   * @param plan
   */
  private static void pushProjection(PlanningContext context,
                                     LogicalNode plan)
      throws CloneNotSupportedException {
    Stack<LogicalNode> stack = new Stack<LogicalNode>();

    ParseTree tree = context.getParseTree();
    QueryBlock block;

    if (tree instanceof QueryBlock) {
      block = (QueryBlock) context.getParseTree();

    } else if (tree instanceof CreateTableStmt) {

      CreateTableStmt createTableStmt = (CreateTableStmt) tree;

      if (createTableStmt.hasQueryBlock()) {
        block = createTableStmt.getSelectStmt();
      } else {
        return;
      }
    } else {

      return;
    }
    OptimizationContext optCtx = new OptimizationContext(context,
        block.getTargetList());

    pushProjectionRecursive(context, optCtx, plan, stack, new HashSet<Column>());
  }

  /**
   * Groupby, Join, and Scan can project necessary columns.
   * This method has three roles:
   * 1) collect column reference necessary for sortkeys, join keys, selection conditions, grouping fields,
   * and having conditions
   * 2) shrink the output schema of each operator so that the operator reduces the output columns according to
   * the necessary columns of their parent operators
   * 3) shrink the input schema of each operator according to the shrunk output schemas of the child operators.
   *
   *
   * @param ctx
   * @param node
   * //@param necessary - columns necessary for above logical nodes, but it excepts the fields for the target list
   * //@param targetList
   * @return
   */
  private static LogicalNode pushProjectionRecursive(
      final PlanningContext ctx, final OptimizationContext optContext,
      final LogicalNode node, final Stack<LogicalNode> stack,
      final Set<Column> necessary) throws CloneNotSupportedException {

    LogicalNode outer;
    LogicalNode inner;
    switch (node.getType()) {
      case ROOT: // It does not support the projection
        LogicalRootNode root = (LogicalRootNode) node;
        stack.add(root);
        outer = pushProjectionRecursive(ctx, optContext,
            root.getSubNode(), stack, necessary);
        root.setInSchema(outer.getOutSchema());
        root.setOutSchema(outer.getOutSchema());
        break;

      case STORE:
        StoreTableNode store = (StoreTableNode) node;
        stack.add(store);
        outer = pushProjectionRecursive(ctx, optContext,
            store.getSubNode(), stack, necessary);
        store.setInSchema(outer.getOutSchema());
        store.setOutSchema(outer.getOutSchema());
        break;

      case PROJECTION:
        ProjectionNode projNode = (ProjectionNode) node;

        stack.add(projNode);
        outer = pushProjectionRecursive(ctx, optContext,
            projNode.getSubNode(), stack, necessary);
        stack.pop();

        LogicalNode childNode = projNode.getSubNode();
        if (optContext.getTargetListManager().isAllEvaluated() // if all exprs are evaluated
            && (childNode.getType() == ExprType.JOIN ||
               childNode.getType() == ExprType.GROUP_BY ||
               childNode.getType() == ExprType.SCAN)) { // if the child node is projectable
            projNode.getSubNode().setOutSchema(
                optContext.getTargetListManager().getUpdatedSchema());
            LogicalNode parent = stack.peek();
            ((UnaryNode)parent).setSubNode(projNode.getSubNode());
            return projNode.getSubNode();
        } else {
          // the output schema is not changed.
          projNode.setInSchema(outer.getOutSchema());
          projNode.setTargetList(
              optContext.getTargetListManager().getUpdatedTarget());
        }
        return projNode;

      case SELECTION: // It does not support the projection
        SelectionNode selNode = (SelectionNode) node;

        if (selNode.getQual() != null) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(selNode.getQual()));
        }

        stack.add(selNode);
        outer = pushProjectionRecursive(ctx, optContext, selNode.getSubNode(),
            stack, necessary);
        stack.pop();
        selNode.setInSchema(outer.getOutSchema());
        selNode.setOutSchema(outer.getOutSchema());
        break;

      case GROUP_BY: {
        GroupbyNode groupByNode = (GroupbyNode)node;

        if (groupByNode.hasHavingCondition()) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(groupByNode.getHavingCondition()));
        }

        stack.add(groupByNode);
        outer = pushProjectionRecursive(ctx, optContext,
            groupByNode.getSubNode(), stack, necessary);
        stack.pop();
        groupByNode.setInSchema(outer.getOutSchema());
        // set all targets
        groupByNode.setTargetList(optContext.getTargetListManager().getUpdatedTarget());

        TargetListManager targets = optContext.getTargetListManager();
        List<Target> groupbyPushable = Lists.newArrayList();
        List<Integer> groupbyPushableId = Lists.newArrayList();

        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (canBeEvaluated(expr, groupByNode) &&
              EvalTreeUtil.findDistinctAggFunction(expr).size() > 0 && expr.getType() != EvalNode.Type.FIELD) {
            targets.setEvaluated(i);
            groupbyPushable.add((Target) targets.getTarget(i).clone());
            groupbyPushableId.add(i);
          }
        }

        return groupByNode;
      }

      case SORT: // It does not support the projection
        SortNode sortNode = (SortNode) node;

        for (SortSpec spec : sortNode.getSortKeys()) {
          necessary.add(spec.getSortKey());
        }

        stack.add(sortNode);
        outer = pushProjectionRecursive(ctx, optContext,
            sortNode.getSubNode(), stack, necessary);
        stack.pop();
        sortNode.setInSchema(outer.getOutSchema());
        sortNode.setOutSchema(outer.getOutSchema());
        break;


      case JOIN: {
        JoinNode joinNode = (JoinNode) node;
        Set<Column> parentNecessary = Sets.newHashSet(necessary);

        if (joinNode.hasJoinQual()) {
          necessary.addAll(EvalTreeUtil.findDistinctRefColumns(joinNode.getJoinQual()));
        }

        stack.add(joinNode);
        outer = pushProjectionRecursive(ctx, optContext,
            joinNode.getOuterNode(), stack, necessary);
        inner = pushProjectionRecursive(ctx, optContext,
            joinNode.getInnerNode(), stack, necessary);
        stack.pop();
        Schema merged = SchemaUtil
            .merge(outer.getOutSchema(), inner.getOutSchema());
        joinNode.setInSchema(merged);

        TargetListManager targets = optContext.getTargetListManager();
        List<Target> joinPushable = Lists.newArrayList();
        List<Integer> joinPushableId = Lists.newArrayList();
        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (canBeEvaluated(expr, joinNode)
              && EvalTreeUtil.findDistinctAggFunction(expr).size() == 0
              && expr.getType() != EvalNode.Type.FIELD) {
            targets.setEvaluated(i);
            joinPushable.add(targets.getTarget(i));
            joinPushableId.add(i);
          }
        }
        if (joinPushable.size() > 0) {
          joinNode.setTargetList(targets.targets);
        }

        Schema outSchema = shrinkOutSchema(joinNode.getInSchema(), targets.getUpdatedSchema().getColumns());
        for (Integer t : joinPushableId) {
          outSchema.addColumn(targets.getEvaluatedColumn(t));
        }
        outSchema = SchemaUtil.mergeAllWithNoDup(outSchema.getColumns(),
            SchemaUtil.getProjectedSchema(joinNode.getInSchema(),parentNecessary).getColumns());
        joinNode.setOutSchema(outSchema);
        break;
      }

      case UNION:  // It does not support the projection
        UnionNode unionNode = (UnionNode) node;
        stack.add(unionNode);

        ParseTree tree =  ctx.getParseTree();
        if (tree instanceof CreateTableStmt) {
          tree = ((CreateTableStmt) tree).getSelectStmt();
        }
        QueryBlock block = (QueryBlock) tree;

        OptimizationContext outerCtx = new OptimizationContext(ctx,
            block.getTargetList());
        OptimizationContext innerCtx = new OptimizationContext(ctx,
            block.getTargetList());
        pushProjectionRecursive(ctx, outerCtx, unionNode.getOuterNode(),
            stack, necessary);
        pushProjectionRecursive(ctx, innerCtx, unionNode.getInnerNode(),
            stack, necessary);
        stack.pop();

        // if this is the final union, we assume that all targets are evalauted
        // TODO - is it always correct?
        if (stack.peek().getType() != ExprType.UNION) {
          optContext.getTargetListManager().setEvaluatedAll();
        }
        break;

      case SCAN: {
        ScanNode scanNode = (ScanNode) node;
        TargetListManager targets = optContext.getTargetListManager();
        List<Integer> scanPushableId = Lists.newArrayList();
        List<Target> scanPushable = Lists.newArrayList();
        EvalNode expr;
        for (int i = 0; i < targets.size(); i++) {
          expr = targets.getTarget(i).getEvalTree();
          if (!targets.isEvaluated(i) && canBeEvaluated(expr, scanNode)) {
            if (expr.getType() == EvalNode.Type.FIELD) {
              targets.setEvaluated(i);
            } else if (EvalTreeUtil.findDistinctAggFunction(expr).size() == 0) {
              targets.setEvaluated(i);
              scanPushable.add(targets.getTarget(i));
              scanPushableId.add(i);
            }
          }
        }

        if (scanPushable.size() > 0) {
          scanNode.setTargets(scanPushable.toArray(new Target[scanPushable.size()]));
        }
        Schema outSchema = shrinkOutSchema(scanNode.getInSchema(), targets.getUpdatedSchema().getColumns());
        for (Integer t : scanPushableId) {
          outSchema.addColumn(targets.getEvaluatedColumn(t));
        }
        outSchema = SchemaUtil.mergeAllWithNoDup(outSchema.getColumns(), SchemaUtil.getProjectedSchema(scanNode.getInSchema(),necessary).getColumns());
        scanNode.setOutSchema(outSchema);

        break;
      }

      default:
    }

    return node;
  }

  private static Schema shrinkOutSchema(Schema inSchema, Collection<Column> necessary) {
    Schema projected = new Schema();
    for(Column col : inSchema.getColumns()) {
      if(necessary.contains(col)) {
        projected.addColumn(col);
      }
    }
    return projected;
  }

  public static class OptimizationContext {
    PlanningContext context;
    TargetListManager targetListManager;

    public OptimizationContext(PlanningContext context, Target [] targets) {
      this.context = context;
      this.targetListManager = new TargetListManager(context, targets);
    }

    public TargetListManager getTargetListManager() {
      return this.targetListManager;
    }
  }

  public static class TargetListManager {
    private PlanningContext context;
    private boolean [] evaluated;
    private Target [] targets;

    public TargetListManager(PlanningContext context, Target [] targets) {
      this.context = context;
      if (targets == null) {
        evaluated = new boolean[0];
      } else {
        evaluated = new boolean[targets.length];
      }
      this.targets = targets;
    }

    public Target getTarget(int id) {
      return targets[id];
    }

    public Target [] getTargets() {
      return this.targets;
    }

    public int size() {
      return targets.length;
    }

    public void setEvaluated(int id) {
      evaluated[id] = true;
    }

    public void setEvaluatedAll() {
      for (int i = 0; i < evaluated.length; i++) {
        evaluated[i] = true;
      }
    }

    private boolean isEvaluated(int id) {
      return evaluated[id];
    }

    public Target [] getUpdatedTarget() throws CloneNotSupportedException {
      Target [] updated = new Target[evaluated.length];
      for (int i = 0; i < evaluated.length; i++) {
        if (evaluated[i]) {
          Column col = getEvaluatedColumn(i);
          updated[i] = new Target(new FieldEval(col), i);
        } else {
          updated[i] = (Target) targets[i].clone();
        }
      }
      return updated;
    }

    public Schema getUpdatedSchema() {
      Schema schema = new Schema();
      for (int i = 0; i < evaluated.length; i++) {
        if (evaluated[i]) {
          Column col = getEvaluatedColumn(i);
          schema.addColumn(col);
        } else {
          Collection<Column> cols = getColumnRefs(i);
          for (Column col : cols) {
            if (!schema.contains(col.getQualifiedName())) {
              schema.addColumn(col);
            }
          }
        }
      }

      return schema;
    }

    public Collection<Column> getColumnRefs(int id) {
      return EvalTreeUtil.findDistinctRefColumns(targets[id].getEvalTree());
    }

    public Column getEvaluatedColumn(int id) {
      Target t = targets[id];
      String name;
      if (t.hasAlias()) {
        name = t.getAlias();
      } else if (t.getEvalTree().getName().equals("?")) {
        name = context.getGeneratedColumnName();
      } else {
        name = t.getEvalTree().getName();
      }
      return new Column(name, t.getEvalTree().getValueType()[0]);
    }

    public boolean isAllEvaluated() {
      for (boolean isEval : evaluated) {
        if (!isEval) {
          return false;
        }
      }

      return true;
    }
  }
}
