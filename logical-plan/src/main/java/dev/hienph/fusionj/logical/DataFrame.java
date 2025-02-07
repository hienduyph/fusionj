package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public interface DataFrame {

  DataFrame project(List<LogicalExpr> expr);

  DataFrame filter(LogicalExpr filter);

  DataFrame aggregate(List<LogicalExpr> groupBy, List<AggregateExpr> aggregate);

  Schema schema();

  LogicalPlan logicalPlan();
}
