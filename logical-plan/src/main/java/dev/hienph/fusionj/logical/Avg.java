package dev.hienph.fusionj.logical;

public class Avg extends AggregateExpr {

  public Avg(LogicalExpr expr) {
    super("AVG", expr);
  }

}
