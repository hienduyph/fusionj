package dev.hienph.fusionj.sql;

public record SqlBinaryExpr(SqlExpr l, String op, SqlExpr r) implements SqlExpr {

  @Override
  public String toString() {
    return String.format("%s %s %s", l, op, r);
  }
}
