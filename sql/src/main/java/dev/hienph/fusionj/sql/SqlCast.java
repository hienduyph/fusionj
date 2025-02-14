package dev.hienph.fusionj.sql;

public record SqlCast(SqlExpr expr, SqlIdentifier dataType) implements SqlExpr {

  @Override
  public String toString() {
    return String.format("CAST(%s AS %s)", expr, dataType);
  }
}
