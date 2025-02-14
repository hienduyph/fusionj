package dev.hienph.fusionj.executor.datasource;

import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ParquetDataSourceTest {

  public static final String dir = "../testdata";

  @Test
  void readParquetFile() {
    var parquet = new ParquetDataSource(new File(dir, "alltypes_plain.parquet").getAbsolutePath());
    var it = parquet.scan(List.of("id")).iterator();
    assert (it.hasNext());
  }
}
