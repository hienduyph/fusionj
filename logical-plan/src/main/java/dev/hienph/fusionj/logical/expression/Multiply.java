package dev.hienph.fusionj.logical.expression;

public class Multiply extends MathExpr {
  public Multiply(LogicalExpr left, LogicalExpr right) {
    super("mult", "*", left, right);
  }
}
