package nta.engine.planner.physical;

import nta.catalog.Schema;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.planner.logical.SelectionNode;
import nta.engine.utils.TupleUtil;
import nta.storage.Tuple;
import nta.storage.VTuple;

import java.io.IOException;

/**
 * @author : hyunsik
 */
public class SelectionExec extends PhysicalExec  {
  private final SelectionNode annotation;
  private final PhysicalExec subOp;
  private final Schema inSchema;
  private final Schema outSchema;

  private final EvalNode qual;
  private final EvalContext qualCtx;
  private final Tuple outputTuple;
  // projection
  private int [] targetIds;

  public SelectionExec(SubqueryContext ctx, SelectionNode selNode,
                       PhysicalExec subOp) {
    this.annotation = selNode;
    this.inSchema = selNode.getInputSchema();
    this.outSchema = selNode.getOutputSchema();
    this.subOp = subOp;

    this.qual = this.annotation.getQual();
    this.qualCtx = this.qual.newContext();
    // for projection
    if (!inSchema.equals(outSchema)) {
      targetIds = TupleUtil.getTargetIds(inSchema, outSchema);
    }

    this.outputTuple = new VTuple(outSchema.getColumnNum());
  }

  @Override
  public Schema getSchema() {
    return annotation.getOutputSchema();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = null;
    while ((tuple = subOp.next()) != null) {
      qual.eval(qualCtx, inSchema, tuple);
      if (qual.terminate(qualCtx).asBool()) {
        if (targetIds != null) {
          TupleUtil.project(tuple, outputTuple, targetIds);
          return outputTuple;
        } else {
          return tuple;
        }
      }
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}
