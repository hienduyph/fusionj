package dev.hienph.fusionj.optimizer;

import dev.hienph.fusionj.logical.LogicalPlan;

public interface OptimizerRule {

  LogicalPlan optimizer(LogicalPlan plan);
}
