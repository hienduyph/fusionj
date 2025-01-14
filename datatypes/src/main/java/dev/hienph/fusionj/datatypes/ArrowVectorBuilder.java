package dev.hienph.fusionj.datatypes;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
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
      case VarCharVector v ->
          v.set(i, value instanceof byte[] ? (byte[]) value : value.toString().getBytes());
      case TinyIntVector v -> {
        if (value instanceof Number) {
          v.set(i, ((Number) value).byteValue());
        } else if (value instanceof String) {
          v.set(i, Integer.parseInt((String) value));
        } else {
          throw new IllegalStateException();
        }
      }
      case SmallIntVector v -> {
        if (value instanceof Number) {
          v.set(i, (short) value);
        } else if (value instanceof String) {
          v.set(i, Short.parseShort((String) value));
        } else {
          throw new IllegalStateException();
        }
      }
      case IntVector v -> {
        if (value instanceof Number) {
          v.set(i, (int) value);
        } else if (value instanceof String) {
          v.set(i, Integer.parseInt((String) value));
        } else {
          throw new IllegalStateException();
        }
      }

      case BigIntVector v -> {
        if (value instanceof Number) {
          v.set(i, (long) value);
        } else if (value instanceof String) {
          v.set(i, Long.parseLong((String) value));
        } else {
          throw new IllegalStateException();
        }
      }

      case Float4Vector v -> {
        if (value instanceof Number) {
          v.set(i, (float) value);
        } else if (value instanceof String) {
          v.set(i, Float.parseFloat((String) value));
        } else {
          throw new IllegalStateException();
        }
      }

      case Float8Vector v -> {
        if (value instanceof Number) {
          v.set(i, (double) value);
        } else if (value instanceof String) {
          v.set(i, Double.parseDouble((String) value));
        } else {
          throw new IllegalStateException();
        }
      }

      default -> throw new IllegalStateException(fieldVector.getClass().getName());
    }
  }
}
