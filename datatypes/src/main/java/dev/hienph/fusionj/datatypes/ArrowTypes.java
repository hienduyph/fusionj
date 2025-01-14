package dev.hienph.fusionj.datatypes;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrowTypes {
  public static final ArrowType BooleanType  = new ArrowType.Bool();
  public static final ArrowType Int8Type = new ArrowType.Int(8, true);
  public static final ArrowType Int16Type = new ArrowType.Int(16, true);
  public static final ArrowType Int32Type = new ArrowType.Int(32, true);
  public static final ArrowType Int64Type = new ArrowType.Int(64, true);
  public static final ArrowType Uint8Type = new ArrowType.Int(8, false);
  public static final ArrowType Uint16Type = new ArrowType.Int(16, false);
  public static final ArrowType Uint32Type = new ArrowType.Int(32, false);
  public static final ArrowType Uint64Type = new ArrowType.Int(64, false);
  public static final ArrowType FloatType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
  public static final ArrowType DoubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
  public static final ArrowType StringType = new ArrowType.Utf8();
}
