package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class AddExpression extends MathExpression {

  public AddExpression(Expression l, Expression r) {
    super(l, r);
  }

  @Override
  Object evaluate(Object l, Object r, ArrowType arrowType) {
    if (arrowType == ArrowTypes.Int8Type) {
      return (Byte) l + (Byte) r;
    }
    if (arrowType == ArrowTypes.Int16Type) {
      return (Short) l + (Short) r;
    }
    if (arrowType == ArrowTypes.Int32Type) {
      return (Integer) l + (Integer) r;
    }
    if (arrowType == ArrowTypes.Int64Type) {
      return (Long) l + (Long) r;
    }
    if (arrowType == ArrowTypes.FloatType) {
      return (Float) l + (Float) r;
    }
    if (arrowType == ArrowTypes.DoubleType) {
      return (Double) l + (Double) r;
    }
    throw new IllegalStateException("unsupported data type in math expression: %s", arrowType);
  }

  @Override
  public String toString() {
    return String.format("%s+%s", l, r);
  }
}
