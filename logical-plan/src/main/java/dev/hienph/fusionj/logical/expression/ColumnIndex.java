package dev.hienph.fusionj.logical.expression;

import dev.hienph.fusionj.datatypes.Field;

public record ColumnIndex(Integer i) implements LogicalExpr {

  @Override
  public Field toField(LogicalPlan input) {
    return input.schema().fields().get(i);
  }

  @Override
  public String toString() {
    return String.format("#%s", i);
  }
}
