package dev.hienph.fusionj.executor.optimizer;

import dev.hienph.fusionj.logical.LogicalPlan;

public interface OptimizerRule {
  LogicalPlan optimizer(LogicalPlan plan);
}
