package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;

public abstract class BooleanBinaryExpr extends BinaryExpr {

  public BooleanBinaryExpr(String name, String op, LogicalExpr left, LogicalExpr right) {
    super(name, op, left, right);
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(this.name, ArrowTypes.BooleanType);
  }
}
