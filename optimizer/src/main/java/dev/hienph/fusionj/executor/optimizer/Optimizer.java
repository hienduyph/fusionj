package dev.hienph.fusionj.executor.optimizer;

import dev.hienph.fusionj.logical.LogicalPlan;

public class Optimizer {
  public LogicalPlan optimize(LogicalPlan plan) {
    var rule = new ProjectionPushDownRule();
    return rule.optimizer(plan);
  }
}
