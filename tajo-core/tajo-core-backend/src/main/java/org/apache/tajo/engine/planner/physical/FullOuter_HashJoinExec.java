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

import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.*;
import org.apache.tajo.datum.*;


public class FullOuter_HashJoinExec extends BinaryPhysicalExec {
  // from logical plan
  protected JoinNode plan;
  protected EvalNode joinQual;

  protected List<Column[]> joinKeyPairs;

  // temporal tuples and states for nested loop join
  protected boolean first = true;
  protected FrameTuple frameTuple;
  protected Tuple outTuple = null;
  protected Map<Tuple, List<Tuple>> tupleSlots;
  protected Iterator<Tuple> iterator = null;
  protected EvalContext qualCtx;
  protected Tuple leftTuple;
  protected Tuple leftKeyTuple;

  protected int [] leftKeyList;
  protected int [] rightKeyList;

  protected boolean finished = false;
  protected boolean shouldGetLeftTuple = true;

  // projection
  protected final Projector projector;
  protected final EvalContext [] evalContexts;

  //camelia --
  private int rightNumCols;
  private int leftNumCols;
  private Map<Tuple, Boolean> matched; 
  //-- camelia

  public FullOuter_HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()),
        plan.getOutSchema(), outer, inner);
    this.plan = plan;
    this.joinQual = plan.getJoinQual();
    this.qualCtx = joinQual.newContext();
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

    //camelia --
    //this hashmap mirrors the evolution of the tupleSlots, with the same keys. For each join key, we have a boolean flag, initially false (whether this join key had at least one match on the left operand)
    this.matched = new HashMap<Tuple, Boolean>(10000);
    //-- camelia

    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual,
        outer.getSchema(), inner.getSchema());

    leftKeyList = new int[joinKeyPairs.size()];
    rightKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      leftKeyList[i] = outer.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      rightKeyList[i] = inner.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
    leftKeyTuple = new VTuple(leftKeyList.length);

    //camelia --
    leftNumCols = outer.getSchema().getColumnNum();
    rightNumCols = inner.getSchema().getColumnNum();
    //-- camelia

  }

  protected void getKeyLeftTuple(final Tuple outerTuple, Tuple keyTuple) {
    for (int i = 0; i < leftKeyList.length; i++) {
      keyTuple.put(i, outerTuple.get(leftKeyList[i]));
    }
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
  
  public Tuple getNextUnmatchedRight() {
     
     List<Tuple> newValue;
     Tuple returnedTuple;
     //get a keyTUple from the matched hashmap with a boolean false value
     for(Tuple aKeyTuple : matched.keySet()) {
        if(matched.get(aKeyTuple) == false) {
          newValue = tupleSlots.get(aKeyTuple); 
          returnedTuple = newValue.remove(0);
          tupleSlots.put(aKeyTuple, newValue);
     
          //after taking the last element from the list in tupleSlots, set flag true in matched as well
          if(newValue.isEmpty()){
             matched.put(aKeyTuple, true);
          }

          return returnedTuple; 
        }
     }
     return null;
         
    
  }
  //-- camelia

  public Tuple next() throws IOException {
    if (first) {
      loadRightToHashTable();
    }

    Tuple rightTuple;
    boolean found = false;

    while(!finished) {

      if (shouldGetLeftTuple) { // initially, it is true.
        // getting new outer
        leftTuple = leftChild.next(); // it comes from a disk
        if (leftTuple == null) { // if no more tuples in left tuples on disk, a join is completed.
          //camelia --
          // in this stage we can begin outputing tuples from the right operand (which were before in tupleSlots) null padded on the left side
          Tuple unmatchedRightTuple = getNextUnmatchedRight();
          if( unmatchedRightTuple == null) {
             finished = true;
             return null;
          }
          else {
             Tuple nullPaddedTuple = createNullPaddedTuple(leftNumCols); 
             frameTuple.set(nullPaddedTuple, unmatchedRightTuple);
             projector.eval(evalContexts, frameTuple);
             projector.terminate(evalContexts, outTuple);
            
             return outTuple;         
          }
          //-- camelia
          
        }

        // getting corresponding right
        getKeyLeftTuple(leftTuple, leftKeyTuple); // get a left key tuple
        if (tupleSlots.containsKey(leftKeyTuple)) { // finds right tuples on in-memory hash table.
          iterator = tupleSlots.get(leftKeyTuple).iterator();
          shouldGetLeftTuple = false;
        } else {
          //camelia --

          //this left tuple doesn't have a match on the right.But full outer join => we should keep it anyway
          //output a tuple with the nulls padded rightTuple
           Tuple nullPaddedTuple = createNullPaddedTuple(rightNumCols); 
           frameTuple.set(leftTuple, nullPaddedTuple);
           projector.eval(evalContexts, frameTuple);
           projector.terminate(evalContexts, outTuple);
           // we simulate we found a match, which is exactly the null padded one
           shouldGetLeftTuple = true;
           return outTuple;         

          //-- camelia

        }
      }

      // getting a next right tuple on in-memory hash table.
      rightTuple = iterator.next();
      frameTuple.set(leftTuple, rightTuple); // evaluate a join condition on both tuples
      joinQual.eval(qualCtx, inSchema, frameTuple); //?? isn't it always true if hash function is identity function??
      if (joinQual.terminate(qualCtx).asBool()) { // if both tuples are joinable
        projector.eval(evalContexts, frameTuple);
        projector.terminate(evalContexts, outTuple);
        found = true;
        //camelia --
        getKeyLeftTuple(leftTuple, leftKeyTuple);
        matched.put(leftKeyTuple, true);
        //-- camelia
      }

      if (!iterator.hasNext()) { // no more right tuples for this hash key
        shouldGetLeftTuple = true;
      }

      if (found) {
        break;
      }
    }

    return outTuple;
  }

  protected void loadRightToHashTable() throws IOException {
    Tuple tuple;
    Tuple keyTuple;

    while ((tuple = rightChild.next()) != null) {
      keyTuple = new VTuple(joinKeyPairs.size());
      List<Tuple> newValue;
      for (int i = 0; i < rightKeyList.length; i++) {
        keyTuple.put(i, tuple.get(rightKeyList[i]));
      }

      if (tupleSlots.containsKey(keyTuple)) {
        newValue = tupleSlots.get(keyTuple);
        newValue.add(tuple);
        tupleSlots.put(keyTuple, newValue);
      } else {
        newValue = new ArrayList<Tuple>();
        newValue.add(tuple);
        tupleSlots.put(keyTuple, newValue);
        //camelia --
        matched.put(keyTuple,false);
        //-- camelia
      }
    }
    first = false;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    tupleSlots.clear();
    first = true;

    finished = false;
    iterator = null;
    shouldGetLeftTuple = true;
  }

  public void close() throws IOException {
    tupleSlots.clear();
  }

  public JoinNode getPlan() {
    return this.plan;
  }
}

