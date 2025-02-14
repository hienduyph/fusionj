package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.executor.datasource.CsvDataSource;
import java.util.List;
import org.junit.jupiter.api.Test;

public class DataFrameTest {

  @Test
  void buildDataFrame() {
    var df = csv()
        .filter(LogicalExpr.col("state").eq(LogicalExpr.lit("CO")))
        .project(
            List.of(LogicalExpr.col("id"),
                LogicalExpr.col("first_name"),
                LogicalExpr.col("last_name")));

    System.out.println(LogicalPlan.format(df.logicalPlan()));
  }

  private DataFrame csv() {
    var employeeCsv = "../testdata/employee.csv";
    return new DataFrameImpl(
        new Scan("employee", new CsvDataSource(employeeCsv, null, true, 1024), List.of()));
  }
}
