package dev.hienph.fusionj.logical;

public class Sum extends AggregateExpr {

  public Sum(LogicalExpr input) {
    super("SUM", input);
  }
}
