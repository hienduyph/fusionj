package dev.hienph.fusionj.executor.datasource;

import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.List;

public interface DataSource {

  Schema schema();

  Sequence<RecordBatch> scan(List<String> projection);
}
