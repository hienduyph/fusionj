package dev.hienph.fusionj.logical;

public class LtEq extends BooleanBinaryExpr {

  public LtEq(LogicalExpr left, LogicalExpr right) {
    super("lteq", "<=", left, right);
  }
}
