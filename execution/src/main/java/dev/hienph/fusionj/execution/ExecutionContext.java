package dev.hienph.fusionj.execution;

import dev.hienph.fusionj.datasource.CsvDataSource;
import dev.hienph.fusionj.datasource.DataSource;
import dev.hienph.fusionj.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.executor.planner.QueryPlanner;
import dev.hienph.fusionj.logical.DataFrame;
import dev.hienph.fusionj.logical.DataFrameImpl;
import dev.hienph.fusionj.logical.LogicalPlan;
import dev.hienph.fusionj.logical.Scan;
import dev.hienph.fusionj.optimizer.Optimizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutionContext {

  private final Map<String, String> settings;
  private final Integer batchSize;
  private final Map<String, DataFrame> tables = HashMap.newHashMap(10);

  public ExecutionContext(Map<String, String> settings) {
    this.settings = settings;
    this.batchSize = Integer.parseInt(settings.getOrDefault("ballista.csv.batchSize", "1024"));
  }


  public DataFrame csv(String filename) {
    return new DataFrameImpl(
      new Scan(filename, new CsvDataSource(filename, null, true, batchSize), List.of()));
  }

  public void register(String tableName, DataFrame df) {
    tables.put(tableName, df);
  }


  public void registerDataSource(String tableName, DataSource dataSource) {
    register(tableName, new DataFrameImpl(new Scan(tableName, dataSource, List.of())));
  }

  public void registerCsv(String tableName, String filename) {
    register(tableName, csv(filename));
  }

  public Sequence<RecordBatch> execute(DataFrame df) {
    return execute(df.logicalPlan());
  }

  public Sequence<RecordBatch> execute(LogicalPlan plan) {
    System.out.println("before optimize");
    System.out.println(plan.pretty());
    plan = new Optimizer().optimize(plan);
    System.out.println("optmized ->");
    System.out.println(plan.pretty());
    final var physicalPlan = new QueryPlanner().createPhysicalPlan(plan);
    System.out.println(physicalPlan.pretty());
    return physicalPlan.execute();
  }
}
