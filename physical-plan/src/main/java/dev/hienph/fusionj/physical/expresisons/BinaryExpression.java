package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;

public abstract class BinaryExpression implements Expression {

  protected final Expression l;
  protected final Expression r;

  public BinaryExpression(Expression l, Expression r) {
    this.r = r;
    this.l = l;
  }

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    var ll = l.evaluate(input);
    var rr = r.evaluate(input);
    if (ll.size() != rr.size()) {
      throw new IllegalStateException(
          String.format("size not match, left %s right %s", ll.size(), rr.size()));
    }
    if (!ll.getType().equals(rr.getType())) {
      throw new IllegalStateException(
          String.format("Binary expression operands do not have the same type %s != %s",
              ll.getType(), rr.getType()));
    }
    return evaluate(ll, rr);
  }

  abstract ColumnVector evaluate(ColumnVector l, ColumnVector r);
}
