package dev.hienph.fusionj.logical.expression;

public abstract class BinaryExpr implements LogicalExpr {

  protected String name;
  protected String op;
  protected LogicalExpr left;
  protected LogicalExpr right;

  public BinaryExpr(String name, String op, LogicalExpr left, LogicalExpr right) {
    this.name = name;
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", left, op, right);
  }
}
