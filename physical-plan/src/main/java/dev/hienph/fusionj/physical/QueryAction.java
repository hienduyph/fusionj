package dev.hienph.fusionj.physical;

import dev.hienph.fusionj.logical.LogicalPlan;

public record QueryAction(LogicalPlan plan) implements Action {
}
