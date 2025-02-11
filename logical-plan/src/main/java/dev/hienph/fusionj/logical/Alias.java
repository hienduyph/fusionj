package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;

public class Alias implements LogicalExpr {

  private final LogicalExpr expr;
  private final String alias;

  public Alias(LogicalExpr expr, String alias) {
    this.alias = alias;
    this.expr = expr;

  }

  @Override
  public Field toField(LogicalPlan input) {
    return new Field(alias, expr.toField(input).dataType());
  }

  @Override
  public String toString() {
    return String.format("%s as %s", expr, alias);
  }

  public LogicalExpr getExpr() {
    return expr;
  }
}

