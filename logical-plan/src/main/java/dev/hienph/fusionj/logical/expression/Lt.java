package dev.hienph.fusionj.logical.expression;

public class Lt extends BooleanBinaryExpr {

  public Lt(LogicalExpr left, LogicalExpr right) {
    super("lt", "<", left, right);
  }
}
