package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public record Limit(LogicalPlan input, Integer limit) implements LogicalPlan {

  @Override
  public Schema schema() {
    return input.schema();
  }

  @Override
  public List<LogicalPlan> children() {
    return List.of(input);
  }

  @Override
  public String toString() {
    return String.format("Limit: %s", limit);
  }
}
