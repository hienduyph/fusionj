package dev.hienph.fusionj.executor.physical.expresisons;

import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;

public record ColumnExpression(Integer i) implements Expression {

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    return input.field(i);
  }

  @Override
  public String toString() {
    return String.format("#%s", i);
  }
}
