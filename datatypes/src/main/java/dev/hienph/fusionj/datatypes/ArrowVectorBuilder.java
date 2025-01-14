package dev.hienph.fusionj.datatypes;

import org.apache.arrow.flatbuf.Null;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;

public record ArrowVectorBuilder(FieldVector fieldVector) {

  public void SetValueCount(Integer n) {
    this.fieldVector.setValueCount(n);
  }

  public ColumnVector build() {
    return new ArrowFieldVector(fieldVector);
  }

  public void set(Integer i, Object value) {
    if (value == null) {
      fieldVector.setNull(i);
      return;
    }
    switch (fieldVector) {
      case VarCharVector v -> v.set(i, value instanceof byte[] ? (byte[]) value : value.toString().getBytes());
      case TinyIntVector v -> {
        if (value instanceof Number) {
          v.set(i, ((Number) value).byteValue());
        } else if (value instanceof String) {
          v.set(i, Integer.parseInt((String)value));
        } else  {
          throw  new IllegalStateException();
        }
      }
      default -> throw new IllegalStateException(fieldVector.getClass().getName());
    }
  }
}
