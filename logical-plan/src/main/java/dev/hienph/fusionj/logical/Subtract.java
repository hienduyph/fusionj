package dev.hienph.fusionj.logical;

public class Subtract extends MathExpr {

  public Subtract(LogicalExpr left, LogicalExpr right) {
    super("subtract", "-", left, right);
  }

}
