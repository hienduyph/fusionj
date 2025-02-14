package dev.hienph.fusionj.executor.optimizer;

import java.util.List;
import java.util.Set;


import dev.hienph.fusionj.logical.Alias;
import dev.hienph.fusionj.logical.BinaryExpr;
import dev.hienph.fusionj.logical.CastExpr;
import dev.hienph.fusionj.logical.Column;
import dev.hienph.fusionj.logical.ColumnIndex;
import dev.hienph.fusionj.logical.LiteralDouble;
import dev.hienph.fusionj.logical.LiteralLong;
import dev.hienph.fusionj.logical.LiteralString;
import dev.hienph.fusionj.logical.LogicalExpr;
import dev.hienph.fusionj.logical.LogicalPlan;

class OptimizerUtils {
  public static void extractColumns(List<LogicalExpr> expr, LogicalPlan plan, Set<String> accum) {
    expr.forEach(it -> extractColumns(it, plan, accum));
  }

  public static void extractColumns(LogicalExpr eexpr, LogicalPlan plan, Set<String> accum ) {
    switch (eexpr) {
      case ColumnIndex expr -> accum.add(plan.schema().fields().get(expr.i()).name());
      case Column expr -> accum.add(expr.name());
      case BinaryExpr expr -> {
        extractColumns(expr.getLeft(), plan, accum);
        extractColumns(expr.getRight(), plan, accum);
      }
      case Alias expr -> extractColumns(expr.getExpr(), plan, accum);
      case CastExpr expr -> extractColumns(expr.expr(), plan, accum);
      case LiteralString  expr -> {}
      case LiteralLong expr -> {}
      case LiteralDouble  expr -> {}
      default -> throw new IllegalStateException(String.format("unsupport column expr %s", eexpr));
    }

  }
}
