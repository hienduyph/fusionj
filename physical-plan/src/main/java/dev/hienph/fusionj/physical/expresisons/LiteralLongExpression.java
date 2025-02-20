package dev.hienph.fusionj.physical.expresisons;


import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.LiteralValueVector;
import dev.hienph.fusionj.datatypes.RecordBatch;

public record LiteralLongExpression(Long value) implements Expression {

  @Override
  public ColumnVector evaluate(RecordBatch input) {
    return new LiteralValueVector(ArrowTypes.Int64Type, value, input.rowCount());
  }
}

