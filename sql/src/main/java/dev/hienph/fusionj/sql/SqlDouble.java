package dev.hienph.fusionj.sql;

public record SqlDouble(Double value) implements SqlExpr {

  @Override
  public String toString() {
    return value.toString();
  }
}
