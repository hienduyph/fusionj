package dev.hienph.fusionj.sql;


public record SqlIdentifier(String id) implements SqlExpr {
  @Override
  public String toString() {
    return id;
  }
}
