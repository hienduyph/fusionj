package dev.hienph.fusionj.logical.expression;

public class Add extends MathExpr {

  public Add(LogicalExpr left, LogicalExpr right) {
    super("add", "+", left, right);
  }

}
