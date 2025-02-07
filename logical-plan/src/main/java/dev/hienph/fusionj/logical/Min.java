package dev.hienph.fusionj.logical;

public class Min extends AggregateExpr {

  public Min(LogicalExpr expr) {
    super("MIN", expr);
  }
}

