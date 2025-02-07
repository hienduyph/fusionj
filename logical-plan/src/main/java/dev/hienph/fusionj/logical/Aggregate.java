package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;
import java.util.stream.Collectors;

public record Aggregate(
    LogicalPlan input,
    List<LogicalExpr> groupExpr,
    List<AggregateExpr> aggregateExpr
) implements LogicalPlan {

  @Override
  public Schema schema() {
    var groups = groupExpr.stream().map(it -> it.toField(input)).collect(Collectors.toList());
    var aggs = aggregateExpr.stream().map(it -> it.toField(input));
    groups.addAll(aggs.toList());
    return new Schema(groups);
  }

  @Override
  public List<LogicalPlan> children() {
    return List.of(input);
  }

  @Override
  public String toString() {
    return String.format("Aggregate: groupExpr=%s, aggregateExpr=%s", groupExpr, aggregateExpr);
  }
}
