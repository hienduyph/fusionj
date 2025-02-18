package dev.hienph.fusionj.datasource;

import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;
import java.util.stream.IntStream;

public record InMemoryDataSource(Schema schema, List<RecordBatch> data) implements DataSource {

  @Override
  public Sequence<RecordBatch> scan(List<String> projection) {
    final var fields = schema.fields();
    final var indices = projection.stream()
      .map(name -> IntStream.range(0, fields.size())
        .filter(idx -> fields.get(idx).name().equals(name))
        .findFirst()
        .orElseThrow()
      ).toList();
    var iter = data.stream().map(batch ->
      new RecordBatch(schema, indices.stream().map(batch::field).toList())
    ).iterator();
    return Sequence.of(iter);
  }

  @Override
  public Schema schema() {
    return schema;
  }
}
