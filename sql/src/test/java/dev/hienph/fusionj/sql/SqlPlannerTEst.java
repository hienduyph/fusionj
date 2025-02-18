package dev.hienph.fusionj.sql;

import dev.hienph.fusionj.datasource.CsvDataSource;
import dev.hienph.fusionj.logical.DataFrame;
import dev.hienph.fusionj.logical.DataFrameImpl;
import dev.hienph.fusionj.logical.LogicalPlan;
import dev.hienph.fusionj.logical.Scan;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlPlannerTEst {

  private final String dir = "../testdata";
  private final String employeeCsv = new File(dir, "employee.csv").getAbsolutePath();

  @Test
  void simpleSelect() {
    var plan = plan("SELECT state FROM employee");
    System.out.println(plan.pretty());
    Assertions.assertEquals("Projection: #state\n" + "\tScan: ; projection=None\n", plan.pretty());
  }

  @Test
  void selectWithFilter() {
    var plan = plan("SELECT state FROM employee WHERE state = 'CA'");
    System.out.println(plan.pretty());
    Assertions.assertEquals(
      "Selection: #state = 'CA'\n" + "\tProjection: #state\n" + "\t\tScan: ; projection=None\n",
      plan.pretty());
  }

  private LogicalPlan plan(String sql) {
    var tokens = new SqlTokenizer(sql).tokenize();
    System.out.println("Tokens " + tokens);
    var parsedQuery = new SqlParser(tokens).parse();
    System.out.println("Ast " + parsedQuery);
    assert parsedQuery != null;

    var datasource = new CsvDataSource(
      employeeCsv, null, true, 1024
    );
    Map<String, DataFrame> tables = Map.of("employee",
      new DataFrameImpl(new Scan("", datasource, List.of())));
    var df = new SqlPlanner().createDataFrame((SqlSelect) parsedQuery, tables);
    return df.logicalPlan();
  }
}
