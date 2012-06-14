/**
 * 
 */
package nta.engine.exec.eval;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class PartialBinaryExpr extends EvalNode {
  
  public PartialBinaryExpr(Type type) {
    super(type);
  }

  public PartialBinaryExpr(Type type, EvalNode left, EvalNode right) {
    super(type, left, right);
  }

  @Override
  public EvalContext newContext() {
    return null;
  }

  @Override
  public DataType getValueType() {
    return null;
  }

  @Override
  public String getName() {
    return "nonamed";
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple, Datum... args) {
    throw new InvalidEvalException(
        "ERROR: the partial binary expression cannot be evluated: "
            + this.toString() );
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    throw new InvalidEvalException(
        "ERROR: the partial binary expression cannot be terminated: "
            + this.toString() );
  }
  
  public String toString() {
    return 
        (leftExpr != null ? leftExpr.toString() : "[EMPTY]") 
        + " " + type + " " 
        + (rightExpr != null ? rightExpr.toString() : "[EMPTY]");
  }
}
