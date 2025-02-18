package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datasource.DataSource;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public record Scan(
  String path,
  DataSource dataSource,
  List<String> projection
) implements LogicalPlan {

  @Override
  public Schema schema() {
    return deriveSchema();
  }

  @Override
  public List<LogicalPlan> children() {
    return List.of();
  }

  @Override
  public String toString() {
    if (projection.isEmpty()) {
      return String.format("Scan: %s; projection=None", path);
    }
    return String.format("Scan: %s; projection=%s", path, projection);
  }

  private Schema deriveSchema() {
    var schema = dataSource.schema();
    if (projection.isEmpty()) {
      return schema;
    }
    return schema.select(projection);
  }
}
