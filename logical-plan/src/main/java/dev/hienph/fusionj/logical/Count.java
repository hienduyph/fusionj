package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;

public class Count extends AggregateExpr {

  public Count(LogicalExpr expr) {
    super("COUNT", expr);
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(name, ArrowTypes.Int32Type);
  }
}
