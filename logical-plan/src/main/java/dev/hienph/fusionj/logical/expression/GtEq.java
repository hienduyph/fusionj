package dev.hienph.fusionj.logical.expression;

public class GtEq extends BooleanBinaryExpr {

  public GtEq(LogicalExpr left, LogicalExpr right) {
    super("gteq", ">=", left, right);
  }
}
