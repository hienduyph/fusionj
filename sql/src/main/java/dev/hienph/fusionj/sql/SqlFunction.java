package dev.hienph.fusionj.sql;

import groovy.sql.Sql;
import java.util.List;

public record SqlFunction(String id, List<SqlExpr> args) implements SqlExpr {

  @Override
  public String toString() {
    return id;
  }
}
