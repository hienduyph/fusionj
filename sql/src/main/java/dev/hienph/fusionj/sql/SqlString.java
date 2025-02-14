package dev.hienph.fusionj.sql;

import groovy.sql.Sql;

public record SqlString(String value) implements SqlExpr  {

  @Override
  public String toString() {
    return String.format("'%s'", value);
  }
}
