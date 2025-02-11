package dev.hienph.fusionj.execution;

import dev.hienph.fusionj.datasource.Sequence;
import dev.hienph.fusionj.logical.LogicalExpr;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutionTest {

  private final String dir = "../testdata";
  private final String employeeCsv = new File(dir, "employee.csv").getAbsolutePath();

  @Test
  void employeeInCOUsingDataFrame() {
    var ctx = new ExecutionContext(Map.of());
    var df = ctx.csv(employeeCsv).filter(LogicalExpr.col("state").eq(LogicalExpr.lit("CO")))
        .project(List.of(LogicalExpr.col("id"), LogicalExpr.col("first_name"),
            LogicalExpr.col("last_name")));
    var batches = Sequence.stream(ctx.execute(df).iterator()).toList();
    Assertions.assertEquals(1, batches.size());
    final var batch = batches.getFirst();
    Assertions.assertEquals("2,Gregg,Langford\n" + "3,John,Travis\n", batch.toCSV());
  }

}
