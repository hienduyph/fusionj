package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.executor.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public interface PhysicalPlan {

  static String format(PhysicalPlan plan, Integer indent) {
    var sb = new StringBuilder();

    sb.append("\t".repeat(Math.max(0, indent)));

    sb.append(plan.toString()).append("\n");
    plan.children().forEach(it -> sb.append(format(it, indent + 1)));
    return sb.toString();
  }

  static String format(PhysicalPlan plan) {
    return format(plan, 0);
  }

  Schema schema();

  Sequence<RecordBatch> execute();

  List<PhysicalPlan> children();

  default String pretty() {
    return format(this);
  }
}
