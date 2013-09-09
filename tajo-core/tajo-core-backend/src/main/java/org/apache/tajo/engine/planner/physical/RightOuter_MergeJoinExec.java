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

import com.google.common.base.Preconditions;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tajo.datum.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RightOuter_MergeJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode joinNode;
  private EvalNode joinQual;
  private EvalContext qualCtx;

  // temporal tuples and states for nested loop join
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private Tuple outerNext = null;

  private final List<Tuple> outerTupleSlots;
  private final List<Tuple> innerTupleSlots;


  private JoinTupleComparator joincomparator = null;
  private TupleComparator[] tupleComparator = null;

  private final static int INITIAL_TUPLE_SLOT = 10000;

  private boolean end = false;

  // projection
  private final Projector projector;
  private final EvalContext [] evalContexts;


  //camelia --
  private int rightNumCols;
  private int leftNumCols;
  private static final Log LOG = LogFactory.getLog(RightOuter_MergeJoinExec.class);
  private int posInnerTupleSlots = -1;
  private int posOuterTupleSlots = -1;
  //-- camelia

  public RightOuter_MergeJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner, SortSpec[] outerSortKey, SortSpec[] innerSortKey) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    Preconditions.checkArgument(plan.hasJoinQual(), "Sort-merge join is only used for the equi-join, " +
        "but there is no join condition");
    this.joinNode = plan;
    this.joinQual = plan.getJoinQual();
    this.qualCtx = this.joinQual.newContext();

    this.outerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    this.innerTupleSlots = new ArrayList<Tuple>(INITIAL_TUPLE_SLOT);
    SortSpec[][] sortSpecs = new SortSpec[2][];
    sortSpecs[0] = outerSortKey;
    sortSpecs[1] = innerSortKey;

    this.joincomparator = new JoinTupleComparator(outer.getSchema(),
        inner.getSchema(), sortSpecs);
    this.tupleComparator = PlannerUtil.getComparatorsFromJoinQual(
        plan.getJoinQual(), outer.getSchema(), inner.getSchema());

    
    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());

    //camelia --
    leftNumCols = outer.getSchema().getColumnNum();
    rightNumCols = inner.getSchema().getColumnNum();
    //-- camelia


  }

  public JoinNode getPlan(){
    return this.joinNode;
  }

  //camelia --
  //creates a tuple of a given size filled with NULL values in all fields
  public Tuple createNullPaddedTuple(int columnNum){
     VTuple aTuple = new VTuple(columnNum);
     int i;
     for(i = 0; i < columnNum; i++){
        aTuple.put(i, DatumFactory.createNullDatum());
     } 
     return aTuple;

  }
  
 
  public Tuple next() throws IOException {
    Tuple previous;
    boolean endInPopulationStage = false;

    for (;;) {
            
      boolean newround = false;
      if((posInnerTupleSlots == -1) && (posOuterTupleSlots == -1))
         newround = true;
      if ((posInnerTupleSlots == innerTupleSlots.size()) && (posOuterTupleSlots == outerTupleSlots.size()))
         newround = true; 

      if(newround == true){
        LOG.info("should start a new round \n");
        if(end){
           
           //before exit,  a leftnullpadded tuple should be built for all remaining right side
           LOG.info(" end is trrue");
           if(innerTuple == null) {  
              return null;
           }
           else {
              //output a tuple with the nulls padded leftTuple
             Tuple nullPaddedTuple = createNullPaddedTuple(leftNumCols); 
             frameTuple.set(nullPaddedTuple, innerTuple);
             projector.eval(evalContexts, frameTuple);
             projector.terminate(evalContexts, outTuple);
             // we simulate we found a match, which is exactly the null padded one           
             LOG.info("********in the end a result null padded tuple =" + outTuple.toString() + "\n");
             innerTuple = rightChild.next();
             if(innerTuple != null)
              LOG.info("********rightChild.next() =" + innerTuple.toString() + "\n");
             else
              LOG.info("********rightChild.next() = NULL\n");
             return outTuple;  
 
            }
            
          
         }//if end

         if(outerTuple == null){
           outerTuple = leftChild.next();
           if( outerTuple != null)
              LOG.info("********leftChild.next() =" + outerTuple.toString() + "\n");
           else
              LOG.info("********leftChild.next() = null \n");
         }
         if(innerTuple == null){
           innerTuple = rightChild.next();
           if(innerTuple != null)
              LOG.info("********rightChild.next() =" + innerTuple.toString() + "\n");
           else
              LOG.info("********rightChild.next() = NULL\n");
          
         }

        outerTupleSlots.clear();
        innerTupleSlots.clear();
        posInnerTupleSlots = -1;
        posOuterTupleSlots = -1;
        

        int cmp;
        while ((cmp = joincomparator.compare(outerTuple, innerTuple)) != 0) {
          
          if (cmp > 0) {
                        
            //before getting a new tuple from the right,  a leftnullpadded tuple should be built
            //output a tuple with the nulls padded leftTuple
            Tuple nullPaddedTuple = createNullPaddedTuple(leftNumCols); 
            frameTuple.set(nullPaddedTuple, innerTuple);
            projector.eval(evalContexts, frameTuple);
            projector.terminate(evalContexts, outTuple);
            // we simulate we found a match, which is exactly the null padded one           
            LOG.info("******** a result null padded tuple =" + outTuple.toString() + "\n");
            // BEFORE RETURN, MOVE FORWARD
            innerTuple = rightChild.next();
            if(innerTuple != null)
               LOG.info("********rightChild.next() =" + innerTuple.toString() + "\n");
            else
                LOG.info("********rightChild.next() = NULL\n");

            if (innerTuple == null){
               end = true;
            }

            return outTuple;  
 
            
            
           } else if (cmp < 0) {
               outerTuple = leftChild.next();
               if( outerTuple != null)
                  LOG.info("********leftChild.next() =" + outerTuple.toString() + "\n");
               else
                  LOG.info("********leftChild.next() = null \n");
           }//cmp<0
          
          
          if (innerTuple == null) {
             return null;
          }

          if (outerTuple == null) {
             
             end = true;
             //in original algorithm we had return null , but now we need to continue the end processing phase for remaining unprocessed right tuples
             break;
          }
        

      }//while

    

     if(end == false) {
           endInPopulationStage = false;
           previous = new VTuple(outerTuple);
           do {
             outerTupleSlots.add(new VTuple(outerTuple));
             outerTuple = leftChild.next();
             if( outerTuple != null)
                LOG.info("********leftChild.next() =" + outerTuple.toString() + "\n");
             else
                LOG.info("********leftChild.next() = null \n");

             if (outerTuple == null) {
               end = true;
               endInPopulationStage = true;
               break;
             }
             
           } while (tupleComparator[0].compare(previous, outerTuple) == 0);
           posOuterTupleSlots = 0;
           

           previous = new VTuple(innerTuple);
           do {
             innerTupleSlots.add(new VTuple(innerTuple));
             innerTuple = rightChild.next();
             if(innerTuple != null)
               LOG.info("********rightChild.next() =" + innerTuple.toString() + "\n");
             else
                LOG.info("********rightChild.next() = NULL\n");

             
             if (innerTuple == null) {
               end = true;
               endInPopulationStage = true;
               break;
             }
                          
           } while (tupleComparator[1].compare(previous, innerTuple) == 0);
           posInnerTupleSlots = 0;
           LOG.info("_________finished populating tuple slots lists in one round ___________");
         } // if end false
       }//if newround
       
         
       if((end == false) && (endInPopulationStage == false)){
         outerNext = new VTuple (outerTupleSlots.get(posOuterTupleSlots));  
         
         if((posInnerTupleSlots == innerTupleSlots.size())&&(posOuterTupleSlots != (outerTupleSlots.size()-1))){
           posOuterTupleSlots = posOuterTupleSlots + 1;
           outerNext = new VTuple(outerTupleSlots.get(posOuterTupleSlots)); 
           posInnerTupleSlots = 0;
           
         }
         

         if((posInnerTupleSlots == innerTupleSlots.size())&&(posOuterTupleSlots == (outerTupleSlots.size()-1))){
           // a new round with new tuples is needed
           posOuterTupleSlots = posOuterTupleSlots + 1;
           continue;
         }


         Tuple aTuple = new VTuple(innerTupleSlots.get(posInnerTupleSlots));
         posInnerTupleSlots = posInnerTupleSlots + 1;
         

         frameTuple.set(outerNext, aTuple);
         joinQual.eval(qualCtx, inSchema, frameTuple);
         projector.eval(evalContexts, frameTuple);
         projector.terminate(evalContexts, outTuple);
         LOG.info("============ result :" + outTuple.toString() + "\n");
         return outTuple;
        
       }//the second if end false

    }//for
  }


  @Override
  public void rescan() throws IOException {
    super.rescan();
    outerTupleSlots.clear();
    innerTupleSlots.clear();
    posInnerTupleSlots = -1;
    posOuterTupleSlots = -1;
  }

  //-- camelia
}

