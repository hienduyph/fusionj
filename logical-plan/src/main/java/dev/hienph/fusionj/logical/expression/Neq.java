package dev.hienph.fusionj.logical.expression;

/*
SQL: a != b
 */
public class Neq extends BooleanBinaryExpr {

  public Neq(LogicalExpr left, LogicalExpr right) {
    super("neq", "!=", left, right);
  }
}
