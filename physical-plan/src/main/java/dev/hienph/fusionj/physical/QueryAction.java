package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.logical.LogicalPlan;

public record QueryAction(LogicalPlan plan) implements Action {
}
