package dev.hienph.fusionj.logical.expression;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;

public record LiteralString(String str) implements LogicalExpr {

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(str, ArrowTypes.StringType);
  }

  @Override
  public String toString() {
    return String.format("'%s'", str);
  }
}
