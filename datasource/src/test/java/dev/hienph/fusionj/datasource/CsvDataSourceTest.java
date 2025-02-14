package dev.hienph.fusionj.executor.datasource;

import com.google.common.collect.Streams;
import dev.hienph.fusionj.datatypes.Field;
import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvDataSourceTest {

  private static final String dir = "../testdata";
  private static final Logger logger = LoggerFactory.getLogger(CsvDataSourceTest.class);

  @Test
  void readCsvWithNoProjection() {
    final var dataFile = new File(dir, "employee.csv").getAbsolutePath();
    System.out.println("test data file " + dataFile);
    logger.info("Data File {}", dataFile);
    var csv = new CsvDataSource(dataFile, null, true, 1024);
    var headers = List.of("id", "first_name", "last_name", "state", "job_title", "salary");
    var result = csv.scan(List.of());
    Streams.stream(result.iterator()).forEach(it -> {
      var field = it.field(0);
      assert (field.size() == 4);
      assert (it.schema().fields().size() == headers.size());
      assert (it.schema().fields().stream().map(Field::name).toList().containsAll(headers));
    });
  }
}
