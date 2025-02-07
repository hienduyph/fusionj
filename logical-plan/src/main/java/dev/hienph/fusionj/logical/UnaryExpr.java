package dev.hienph.fusionj.logical;

public abstract class UnaryExpr implements LogicalExpr {

  String name;
  String op;
  LogicalExpr expr;

  public UnaryExpr(
      String name,
      String op,
      LogicalExpr expr
  ) {
    this.name = name;
    this.op = op;
    this.expr = expr;
  }


  @Override
  public String toString() {
    return String.format("%s %s", name, op);
  }
}
