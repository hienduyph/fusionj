package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public record DataFrameImpl(LogicalPlan plan) implements DataFrame {

  @Override
  public DataFrame project(List<LogicalExpr> expr) {
    return new DataFrameImpl(new Projection(plan, expr));
  }

  @Override
  public DataFrame filter(LogicalExpr filter) {
    return new DataFrameImpl(new Selection(plan, filter));
  }

  @Override
  public DataFrame aggregate(List<LogicalExpr> groupBy, List<AggregateExpr> aggregate) {
    return new DataFrameImpl(new Aggregate(plan, groupBy, aggregate));
  }

  @Override
  public Schema schema() {
    return plan.schema();
  }

  @Override
  public LogicalPlan logicalPlan() {
    return plan;
  }
}
