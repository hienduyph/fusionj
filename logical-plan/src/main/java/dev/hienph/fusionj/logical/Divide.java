package dev.hienph.fusionj.logical;

public class Divide extends MathExpr {

  public Divide(LogicalExpr left, LogicalExpr right) {
    super("div", "/", left, right);
  }
}
