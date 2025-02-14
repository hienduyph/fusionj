package dev.hienph.fusionj.executor.datasource.utils;

import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.RecordBatch;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;

public class ParquetIterator implements Iterator<RecordBatch> {

  private final ParquetFileReader reader;
  private final List<String> projectedColumns;
  private final Schema projectedArrowSchema;
  private Optional<RecordBatch> batch;

  public ParquetIterator(ParquetFileReader reader, List<String> projectedColumns) {
    this.reader = reader;
    this.projectedColumns = projectedColumns;
    var parquetSchema = reader.getFooter().getFileMetaData().getSchema();
    var arrowSchema = (new SchemaConverter()).fromParquet(parquetSchema).getArrowSchema();
    projectedArrowSchema = new Schema(
        projectedColumns.stream().map(arrowSchema::findField).toList());
  }

  @Override
  public boolean hasNext() {
    try {
      batch = nextBatch();
      return batch.isPresent();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public RecordBatch next() {
    var next = batch.orElseThrow();
    batch = Optional.empty();
    return next;
  }

  Optional<RecordBatch> nextBatch() throws IOException {
    var pages = reader.readNextRowGroup();
    if (pages == null) {
      return Optional.empty();
    }
    if (pages.getRowCount() > Integer.MAX_VALUE) {
      throw new IllegalThreadStateException();
    }
    int rows = Long.valueOf(pages.getRowCount()).intValue();
    var root = VectorSchemaRoot.create(projectedArrowSchema, new RootAllocator(Long.MAX_VALUE));
    root.allocateNew();
    root.setRowCount(rows);

    var ballistaSchema = dev.hienph.fusionj.datatypes.SchemaConverter.fromArrow(
        projectedArrowSchema);

    var batchField = root.getFieldVectors().stream().map(ArrowFieldVector::new).toList();
    batch = Optional.of(new RecordBatch(
        ballistaSchema,
        batchField));
    for (int rowIndex = 0; rowIndex < rows; rowIndex++) {
      for (int projectionIndex = 0; projectionIndex < projectedColumns.size(); projectionIndex++) {
      }

    }
    return batch;
  }
}
