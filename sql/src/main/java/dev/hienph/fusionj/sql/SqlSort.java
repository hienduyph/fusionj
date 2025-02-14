package dev.hienph.fusionj.sql;

public record SqlSort(SqlExpr expr, Boolean asc) implements  SqlExpr {

  @Override
  public String toString() {
    return String.format("ORDER BY %s %s", expr, asc ? "ASC" :  "DESC");
  }
}
