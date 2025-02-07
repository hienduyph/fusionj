package dev.hienph.fusionj.logical;

public class Lt extends BooleanBinaryExpr {

  public Lt(LogicalExpr left, LogicalExpr right) {
    super("lt", "<", left, right);
  }
}
