package dev.hienph.fusionj.logical;

public class Max extends AggregateExpr {

  public Max(LogicalExpr expr) {
    super("MAX", expr);
  }
}
