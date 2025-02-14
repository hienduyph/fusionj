package dev.hienph.fusionj.executor.optimizer;

import dev.hienph.fusionj.logical.Aggregate;
import dev.hienph.fusionj.logical.AggregateExpr;
import dev.hienph.fusionj.logical.LogicalPlan;
import dev.hienph.fusionj.logical.Projection;
import dev.hienph.fusionj.logical.Scan;
import dev.hienph.fusionj.logical.Selection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ProjectionPushDownRule implements OptimizerRule {

  @Override
  public LogicalPlan optimizer(LogicalPlan plan) {
    return pushDown(plan, new HashSet<>());
  }


  private LogicalPlan pushDown(LogicalPlan pplan, Set<String> columnNames) {
    return switch (pplan) {
      case Projection plan -> {
        OptimizerUtils.extractColumns(plan.expr(), plan.input(), columnNames);
        var input = pushDown(plan.input(), columnNames);
        yield new Projection(input, plan.expr());
      }
      case Selection plan -> {
        OptimizerUtils.extractColumns(plan.expr(), plan.input(), columnNames);
        var input = pushDown(plan.input(), columnNames);
        yield new Selection(input, plan.expr());
      }
      case Aggregate plan -> {
        OptimizerUtils.extractColumns(plan.groupExpr(), plan.input(), columnNames);
        OptimizerUtils.extractColumns(
          plan.aggregateExpr().stream().map(AggregateExpr::getExpr).toList(), plan.input(),
          columnNames);
        var input = pushDown(plan.input(), columnNames);
        yield new Aggregate(input, plan.groupExpr(), plan.aggregateExpr());
      }
      case Scan plan -> {
        var validFields = plan.dataSource().schema().fields().stream().map(it -> it.name())
          .collect(Collectors.toSet());
        var pushDown = validFields.stream().filter(it -> columnNames.contains(it))
          .collect(Collectors.toSet()).stream().sorted().toList();
        yield new Scan(plan.path(), plan.dataSource(), pushDown);
      }
      default -> throw new IllegalStateException(
        String.format("ProjectionPushDownRule does not support plan %s", pplan));
    };

  }

}
