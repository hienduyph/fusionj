package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;
import java.util.stream.Collectors;

public record Projection(LogicalPlan input, List<LogicalExpr> expr) implements LogicalPlan {

  @Override
  public Schema schema() {
    return new Schema(expr.stream().map(t -> t.toField(input)).collect(Collectors.toList()));
  }

  @Override
  public List<LogicalPlan> children() {
    return List.of(input);
  }

  @Override
  public String toString() {
    var projs = expr.stream().map(Object::toString).collect(Collectors.joining(", "));
    return String.format("Projection: %s", projs);
  }
}
