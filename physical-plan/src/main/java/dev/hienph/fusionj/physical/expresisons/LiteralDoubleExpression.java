package dev.hienph.fusionj.physical.expresisons;


import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.LiteralValueVector;
import dev.hienph.fusionj.datatypes.RecordBatch;

public record LiteralDoubleExpression(Double value) implements Expression {

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    return new LiteralValueVector(ArrowTypes.DoubleType, value, input.rowCount());
  }
}
