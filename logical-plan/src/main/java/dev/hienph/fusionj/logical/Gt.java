package dev.hienph.fusionj.logical;

public class Gt extends BooleanBinaryExpr {

  public Gt(LogicalExpr left, LogicalExpr right) {
    super("gt", ">", left, right);
  }
}
