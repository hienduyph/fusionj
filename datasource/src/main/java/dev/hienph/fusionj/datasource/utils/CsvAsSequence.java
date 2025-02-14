package dev.hienph.fusionj.executor.datasource.utils;

import com.univocity.parsers.csv.CsvParser;
import dev.hienph.fusionj.executor.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.Iterator;

public record CsvAsSequence(
    Schema schema,
    CsvParser parser,
    Integer batchSize
) implements Sequence<RecordBatch> {

  @Override
  public Iterator<RecordBatch> iterator() {
    return new CsvIterator(schema, parser, batchSize);
  }
}
