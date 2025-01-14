package dev.hienph.fusionj.datatypes;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

public record ArrowFieldVector(FieldVector field) implements ColumnVector {

  @Override
  public ArrowType getType() {
    return switch ( field) {
      case BitVector ignored -> ArrowTypes.BooleanType;
      case TinyIntVector ignored -> ArrowTypes.Int8Type;
      case SmallIntVector ignored -> ArrowTypes.Int16Type;
      case IntVector ignored -> ArrowTypes.Int32Type;
      case BigIntVector  ignored ->  ArrowTypes.Int64Type;
      default ->  throw new IllegalStateException();
    };
  }

  @Override
  public Object getValue(Integer i) {
    if (field.isNull(i)) {
      return null;
    }

    return switch (field) {
      case BitVector b -> b.get(i) == 1;
      case TinyIntVector b -> b.get(i);
      case SmallIntVector b -> b.get(i);
      case IntVector b -> b.get(i);
      case BigIntVector b -> b.get(i);
      case Float4Vector b -> b.get(i);
      case Float8Vector b -> b.get(i);
      case VarCharVector b -> b.isNull(i) ? null : new String(b.get(i)) ;
      default -> throw new IllegalStateException();
    };
  }

  @Override
  public Integer size() {
    return field.getValueCount();
  }
}
