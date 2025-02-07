package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;

public abstract class MathExpr extends BinaryExpr {

  public MathExpr(String name, String op, LogicalExpr left, LogicalExpr right) {
    super(name, op, left, right);
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field("mult", left.toField(input).dataType());
  }
}
