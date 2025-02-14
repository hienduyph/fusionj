package dev.hienph.fusionj.executor.physical.expresisons;

import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;
import java.util.stream.IntStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;

public abstract class UnaryMathExpression implements Expression {

  protected Expression expr;

  public UnaryMathExpression(Expression expr) {
    this.expr = expr;
  }

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    var n = expr.evaluate(input);
    var v = new Float8Vector("v", new RootAllocator(Long.MAX_VALUE));
    v.allocateNew();
    IntStream.range(0, n.size()).forEach(it -> {
      var nv = n.getValue(it);
      if (nv == null) {
        v.setNull(it);
      } else if (nv instanceof Double) {
        v.set(it, apply((Double) nv));
      } else {
        throw new UnsupportedOperationException("math expr only apply for double type");
      }
    });
    return new ArrowFieldVector(v);
  }

  abstract Double apply(Double value);
}
