package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.datatypes.ArrowVectorBuilder;
import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.FieldVectorFactory;
import java.util.stream.IntStream;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class MathExpression extends BinaryExpression {
  public MathExpression(Expression l, Expression r) {
    super(l, r);
  }

  @Override
  ColumnVector evaluate(ColumnVector l, ColumnVector r) {
    var fieldVector = FieldVectorFactory.create(l.getType(), l.size());
    var builder = new ArrowVectorBuilder(fieldVector);
    IntStream.range(0, l.size()).forEach( it -> {
      var value = evaluate(l.getValue(it), r.getValue(it), l.getType());
      builder.set(it, value);
    });
    builder.setValueCount(l.size());
    return  builder.build();
  }

  abstract  Object evaluate(Object l, Object r, ArrowType arrowType);
}
