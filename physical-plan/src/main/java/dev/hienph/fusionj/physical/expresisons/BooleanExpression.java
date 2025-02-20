package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;
import java.util.Objects;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class BooleanExpression implements Expression {

  protected final Expression left;
  protected final Expression right;

  public BooleanExpression(Expression left, Expression right) {
    this.left = left;
    this.right = right;
  }

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    var ll = left.evaluate(input);
    var rr = right.evaluate(input);
    if (!Objects.equals(ll.size(), rr.size())) {
      throw new IllegalStateException("size not match");
    }
    if (!ll.getType().equals(rr.getType())) {
      throw new IllegalStateException("type mismatch");
    }
    return compare(ll, rr);
  }

  ColumnVector compare(ColumnVector left, ColumnVector right) {
    var v = new BitVector("v", new RootAllocator(Long.MAX_VALUE));
    v.allocateNew();
    for (var i = 0; i < left.size(); i++) {
      var value = evaluate(left.getValue(i), right.getValue(i), left.getType());
      v.set(i, value ? 1 : 0);
    }
    v.setValueCount(left.size());
    return new ArrowFieldVector(v);
  }

  abstract boolean evaluate(Object l, Object r, ArrowType arrowType);
}
