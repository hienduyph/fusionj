package dev.hienph.fusionj.logical;

/*
`=` op
 */
public class Eq extends BooleanBinaryExpr {

  public Eq(LogicalExpr left, LogicalExpr right) {
    super("eq", "=", left, right);
  }
}
