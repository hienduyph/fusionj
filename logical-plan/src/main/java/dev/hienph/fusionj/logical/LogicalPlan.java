package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public interface LogicalPlan {

  static String format(LogicalPlan plan, Integer indent) {
    var sb = new StringBuilder();
    for (var i = 0; i < indent; i++) {
      sb.append("\t");
    }
    sb.append(plan.toString()).append("\n");
    plan.children().forEach(c -> sb.append(format(c, indent + 1)));
    return sb.toString();
  }

  static String format(LogicalPlan plan) {
    return format(plan, 0);
  }

  Schema schema();

  List<LogicalPlan> children();

  default String pretty() {
    return format(this);
  }
}
