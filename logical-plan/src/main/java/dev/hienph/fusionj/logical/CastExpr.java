package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

public record CastExpr(LogicalExpr expr, ArrowType dataType) implements LogicalExpr {

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(expr.toField(input).name(), dataType);
  }

  @Override
  public String toString() {
    return String.format("CAST(%s AS %s)", expr, dataType);
  }
}
