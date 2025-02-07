package dev.hienph.fusionj.logical;

import dev.hienph.fusionj.datasource.CsvDataSource;
import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

public class LogicalPlanTest {

  private final String dir = "../testdata";
  private final String employeeCsv = new File(dir, "employee.csv").getAbsolutePath();


  @Test
  void buildLogicalPlanManually() {
    var csv = new CsvDataSource(employeeCsv, null, true, 10);
    var scan = new Scan("employee", csv, List.of());
    var filterExpr = LogicalExpr.eq(LogicalExpr.col("state"), LogicalExpr.lit("CO"));
    var selection = new Selection(scan, filterExpr);
    var plan = new Projection(selection,
        List.of(LogicalExpr.col("id"), LogicalExpr.col("first_name"),
            LogicalExpr.col("last_name")));

    System.out.println(LogicalPlan.format(plan));

  }
}
