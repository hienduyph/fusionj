package dev.hienph.fusionj.logical;

public class GtEq extends BooleanBinaryExpr {

  public GtEq(LogicalExpr left, LogicalExpr right) {
    super("gteq", ">=", left, right);
  }
}
