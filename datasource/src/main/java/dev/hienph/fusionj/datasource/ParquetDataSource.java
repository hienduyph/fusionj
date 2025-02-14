package dev.hienph.fusionj.executor.datasource;

import dev.hienph.fusionj.executor.datasource.utils.ParquetScan;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;
import org.apache.parquet.arrow.schema.SchemaConverter;

public record ParquetDataSource(String filename) implements DataSource {

  @Override
  public Sequence<RecordBatch> scan(List<String> projection) {
    return new ParquetScan(filename, projection);
  }

  @Override
  public Schema schema() {
    var sc = new ParquetScan(filename, List.of());
    var arrowSchema = new SchemaConverter().fromParquet(sc.getSchema()).getArrowSchema();
    return dev.hienph.fusionj.datatypes.SchemaConverter.fromArrow(arrowSchema);
  }
}
