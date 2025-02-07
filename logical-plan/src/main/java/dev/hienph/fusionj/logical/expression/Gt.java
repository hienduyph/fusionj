package dev.hienph.fusionj.logical.expression;

public class Gt extends BooleanBinaryExpr {

  public Gt(LogicalExpr left, LogicalExpr right) {
    super("gt", ">", left, right);
  }
}
