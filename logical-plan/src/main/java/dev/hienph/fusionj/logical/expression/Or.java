package dev.hienph.fusionj.logical.expression;


public class Or extends BooleanBinaryExpr {

  public Or(LogicalExpr left, LogicalExpr right) {
    super("or", "OR", left, right);
  }

}
