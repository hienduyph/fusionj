package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;

public record CastExpression(
        Expression expr,
        ArrowType datatype
) implements Expression {
    @Override
    public String toString() {
        return String.format("CAST(%s AS %s)", expr, datatype);
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return null;
    }
}
