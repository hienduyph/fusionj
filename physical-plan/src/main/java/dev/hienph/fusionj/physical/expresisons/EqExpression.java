package dev.hienph.fusionj.physical.expresisons;

import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class EqExpression extends BooleanExpression {

  public EqExpression(Expression l, Expression r) {
    super(l, r);
  }

  @Override
  boolean evaluate(Object l, Object r, ArrowType arrowType) {
    return Objects.equals(l, r);
  }

  @Override
  public String toString() {
    return String.format("%s=%s", left, right);
  }
}
