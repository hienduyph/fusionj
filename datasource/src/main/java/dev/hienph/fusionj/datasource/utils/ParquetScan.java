package dev.hienph.fusionj.executor.datasource.utils;

import dev.hienph.fusionj.executor.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetScan implements AutoCloseable,
    Sequence<RecordBatch> {

  private final MessageType schema;
  private List<String> columns;
  private ParquetFileReader reader;

  public ParquetScan(String filename, List<String> columns) {
    try {
      reader = ParquetFileReader.open(
          HadoopInputFile.fromPath(new Path(filename), new Configuration()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    schema = reader.getFooter().getFileMetaData().getSchema();
    this.columns = columns;
  }

  public MessageType getSchema() {
    return schema;
  }

  @Override
  public Iterator<RecordBatch> iterator() {
    return new ParquetIterator(reader, columns);
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}
