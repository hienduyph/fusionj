package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class CountDistinct extends AggregateExpr {

  public CountDistinct(LogicalExpr expr) {
    super("COUNT DISTINCT", expr);
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(name, new ArrowType.Int(32, false));
  }

  @Override
  public String toString() {
    return String.format("COUNT(DISTINCT %s)", expr);
  }
}
