package dev.hienph.fusionj.datatypes;

import java.util.List;

public record RecordBatch(Schema schema, List<ColumnVector> fields) {
  public Integer rowCount() {
    return  fields.getFirst().size();
  }

  public  Integer columnCount() {
    return fields.size();
  }

  public  ColumnVector field(Integer i) {
    return fields.get(i);
  }

  @Override
  public String toString() {
    return toCSV();
  }

  public  String toCSV() {
    var sb = new StringBuilder();
    final var columnCount = schema.fields().size();
    for (var rowIndex = 0; rowIndex < rowCount(); rowIndex++) {
      for (var columnIndex = 0; columnIndex < columnCount; columnIndex ++) {
        if (columnIndex > 0) {
          sb.append(",");
        }
        var col = field(columnIndex);
        var value = col.getValue(rowIndex);
        if (value == null) {
          sb.append("null");
        } else if (value instanceof byte[]) {
          sb.append(String.valueOf(value));
        } else {
          sb.append(value);
        }
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
