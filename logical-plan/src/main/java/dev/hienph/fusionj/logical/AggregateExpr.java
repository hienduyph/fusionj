package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;

public abstract class AggregateExpr implements LogicalExpr {

  protected final String name;
  protected final LogicalExpr expr;

  public AggregateExpr(String name, LogicalExpr expr) {
    this.expr = expr;
    this.name = name;
  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(name, expr.toField(input).dataType());
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", name, expr);
  }

  public LogicalExpr getExpr() {
    return expr;
  }
}
