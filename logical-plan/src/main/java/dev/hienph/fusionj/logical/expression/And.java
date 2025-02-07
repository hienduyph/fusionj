package dev.hienph.fusionj.logical.expression;

public class And extends BooleanBinaryExpr {

  public And(LogicalExpr left, LogicalExpr right) {
    super("and", "AND", left, right);
  }
}
