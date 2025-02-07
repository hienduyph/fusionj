package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;

public record LiteralDouble(Double val) implements LogicalExpr {

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(val.toString(), ArrowTypes.DoubleType);
  }

  @Override
  public String toString() {
    return val.toString();
  }
}
