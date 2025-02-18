package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import dev.hienph.fusionj.executor.physical.expresisons.Expression;
import java.util.List;

public record ProjectionExec(PhysicalPlan input, Schema schema, List<Expression> expr) implements
  PhysicalPlan {

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public List<PhysicalPlan> children() {
    return List.of(input);
  }

  @Override
  public Sequence<RecordBatch> execute() {
    var r = Sequence.stream(input.execute().iterator()).map(batch -> {
      var columns = expr.stream().map(it -> it.evaluate(batch));
      return new RecordBatch(schema, columns.toList());
    });
    return Sequence.of(r.iterator());
  }

  @Override
  public String toString() {
    return String.format("ProjectionExec: %s", expr);
  }
}
