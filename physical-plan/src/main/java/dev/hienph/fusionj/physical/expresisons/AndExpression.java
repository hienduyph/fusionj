package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.physical.utils.Converts;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class AndExpression extends BooleanExpression {

  public AndExpression(Expression l, Expression r) {
    super(l, r);
  }

  @Override
  boolean evaluate(Object l, Object r, ArrowType arrowType) {
    return Converts.toBool(l) && Converts.toBool(r);
  }
}
