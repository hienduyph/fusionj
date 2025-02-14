package dev.hienph.fusionj.sql;

public record SqlLong(Long value) implements SqlExpr {

  @Override
  public String toString() {
    return value.toString();
  }
}
