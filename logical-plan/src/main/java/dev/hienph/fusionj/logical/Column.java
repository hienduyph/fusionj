package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Field;
import java.sql.SQLException;

public record Column(String name) implements LogicalExpr {

  @Override
  public Field toField(LogicalPlan input) {
    var field = input.schema().fields().stream().filter(t -> t.name().equals(name)).findFirst();
    if (field.isEmpty()) {
      throw new RuntimeException(new SQLException(String.format("No Column name %s", name)));
    }
    return field.get();
  }

  @Override
  public String toString() {
    return String.format("#%s", name);
  }
}
