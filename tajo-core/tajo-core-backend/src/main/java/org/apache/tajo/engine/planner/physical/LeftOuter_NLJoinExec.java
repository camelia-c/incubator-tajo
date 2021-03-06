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
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import org.apache.tajo.datum.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LeftOuter_NLJoinExec extends BinaryPhysicalExec {
  // from logical plan
  private JoinNode plan;
  private EvalNode joinQual;


  // temporal tuples and states for nested loop join
  private boolean needNewOuter;
  private FrameTuple frameTuple;
  private Tuple outerTuple = null;
  private Tuple innerTuple = null;
  private Tuple outTuple = null;
  private EvalContext qualCtx;

  // projection
  private final EvalContext [] evalContexts;
  private final Projector projector;

  //camelia --
  private boolean foundAtLeastOneMatch;
  private int rightNumCols;
  private int leftNumCols;
  private static final Log LOG = LogFactory.getLog(LeftOuter_NLJoinExec.class);
  //-- camelia

  public LeftOuter_NLJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer,
      PhysicalExec inner) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
    this.plan = plan;

    if (plan.hasJoinQual()) {
      this.joinQual = plan.getJoinQual();
      this.qualCtx = this.joinQual.newContext();
    }

    // for projection
    projector = new Projector(inSchema, outSchema, plan.getTargets());
    evalContexts = projector.renew();

    // for join
    needNewOuter = true;
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());

    //camelia --
    foundAtLeastOneMatch = false;
    leftNumCols = outer.getSchema().getColumnNum();
    rightNumCols = inner.getSchema().getColumnNum();
    LOG.info("******** leftNumCols=" + leftNumCols + " rightNumCols=" + rightNumCols + "\n");
    //-- camelia
  }

  public JoinNode getPlan() {
    return this.plan;
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
  //-- camelia

  public Tuple next() throws IOException {
    for (;;) {
      if (needNewOuter) {
        outerTuple = leftChild.next();
        if( outerTuple != null)
           LOG.info("********leftChild.next() =" + outerTuple.toString() + "\n");
        else
           LOG.info("********leftChild.next() = null \n");        

        if (outerTuple == null) {
          return null;
        }
        needNewOuter = false;

        //camelia --
        // a new tuple from the left child has initially no matches on the right operand
        foundAtLeastOneMatch = false;
        //-- camelia
      }

      innerTuple = rightChild.next();
      if(innerTuple != null)
         LOG.info("********rightChild.next() =" + innerTuple.toString() + "\n");
      else
         LOG.info("********rightChild.next() = NULL\n");


      if (innerTuple == null) {


        //camelia --
        // the scan of the right operand is finished with no matches found
        if(foundAtLeastOneMatch == false){
           //output a tuple with the nulls padded rightTuple
           Tuple nullPaddedTuple = createNullPaddedTuple(rightNumCols); 
           frameTuple.set(outerTuple, nullPaddedTuple);
           projector.eval(evalContexts, frameTuple);
           projector.terminate(evalContexts, outTuple);
           // we simulate we found a match, which is exactly the null padded one
           LOG.info("******** a result null padded tuple =" + outTuple.toString() + "\n");
           foundAtLeastOneMatch = true;
           needNewOuter = true;
           rightChild.rescan();
           return outTuple;
        }
        else {
           needNewOuter = true;
           rightChild.rescan();
           continue;

        }
        //-- camelia

      }

      frameTuple.set(outerTuple, innerTuple);
      //for outer join, the joinQual is always not null
      if (joinQual != null) {
        joinQual.eval(qualCtx, inSchema, frameTuple);
        if (joinQual.terminate(qualCtx).asBool()) {
          projector.eval(evalContexts, frameTuple);
          projector.terminate(evalContexts, outTuple);
          //camelia --
          foundAtLeastOneMatch = true;
          LOG.info("******** a result matched padded tuple =" + outTuple.toString() + "\n");
          //-- camelia
          return outTuple;
        }
      } 


    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    needNewOuter = true;
  }
}
