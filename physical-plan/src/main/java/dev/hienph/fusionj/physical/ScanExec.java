package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.executor.datasource.DataSource;
import dev.hienph.fusionj.executor.datasource.Sequence;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;

import java.util.List;

public record ScanExec(
        DataSource ds,
        List<String> projection
) implements PhysicalPlan {
    @Override
    public Schema schema() {
        return ds.schema().select(projection);
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of();
    }

    @Override
    public Sequence<RecordBatch> execute() {
        return ds.scan(projection);
    }

    @Override
    public String toString() {
        return String.format("ScanExec: schema=%s, projection=%s", schema(), projection);
    }
}
