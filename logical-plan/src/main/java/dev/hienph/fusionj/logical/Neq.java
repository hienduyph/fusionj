package dev.hienph.fusionj.logical;

/*
SQL: a != b
 */
public class Neq extends BooleanBinaryExpr {

  public Neq(LogicalExpr left, LogicalExpr right) {
    super("neq", "!=", left, right);
  }
}
