package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.datasource.Sequence;
import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.ArrowVectorBuilder;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import dev.hienph.fusionj.executor.physical.expresisons.Accumulator;
import dev.hienph.fusionj.executor.physical.expresisons.AggregateExpression;
import dev.hienph.fusionj.executor.physical.expresisons.Expression;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public record HashAggregateExec(
  PhysicalPlan input,
  List<Expression> groupExpr,
  List<AggregateExpression> aggregateExpr,
  Schema schema
) implements PhysicalPlan {

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public List<PhysicalPlan> children() {
    return List.of(input);
  }

  @Override
  public String toString() {
    return String.format("HashAggregateExec: groupExpr=%s, aggrExpr=%s", groupExpr, aggregateExpr);
  }

  @Override
  public Sequence<RecordBatch> execute() {
    var map = new HashMap<List<Object>, List<Accumulator>>();
    var ss = StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(input.execute().iterator(), Spliterator.ORDERED), false);
    ss.forEach(batch -> {
      var groupKeys = groupExpr.stream().map(it -> it.evaluate(batch)).toList();
      var aggInputValues = aggregateExpr.stream().map(it -> it.inputExpression().evaluate(batch))
        .toList();
      // loop records in batch
      for (var rowIndex = 0; rowIndex < batch.rowCount(); rowIndex += 1) {
        int finalRowIndex = rowIndex;
        var rowKey = groupKeys.stream().map(it -> {
          var value = it.getValue(finalRowIndex);
          if (value instanceof byte[]) {
            return new String((byte[]) value);
          }
          return value;
        }).toList();

        // get or create accumulators for this grouping key
        var accumulators = map.get(rowKey);
        if (accumulators == null) {
          accumulators = aggregateExpr.stream().map(AggregateExpression::createAccumulator)
            .toList();
        }
        List<Accumulator> finalAccumulators = accumulators;
        IntStream.range(0, accumulators.size()).forEach(idx -> {
          var value = aggInputValues.get(idx).getValue(finalRowIndex);
          finalAccumulators.get(idx).accumulate(value);
        });
        map.put(rowKey, finalAccumulators);
      }

    });

    // create result batch containing the final aggregate values
    var root = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator(Long.MAX_VALUE));
    root.allocateNew();
    root.setRowCount(map.size());
    var builders = root.getFieldVectors().stream().map(ArrowVectorBuilder::new).toList();
    var entrySet = map.entrySet().stream().toList();
    IntStream.range(0, map.size()).forEach(rowIndex -> {
      var groupingKey = entrySet.get(rowIndex).getKey();
      var accumulators = entrySet.get(rowIndex).getValue();
      IntStream.range(0, groupExpr.size())
        .forEach(it -> builders.get(it).set(rowIndex, groupingKey.get(it)));
      IntStream.range(0, aggregateExpr.size()).forEach(
        it -> builders.get(groupExpr.size() + it).set(rowIndex, accumulators.get(it).finalValue()));
    });
    var cols = root.getFieldVectors().stream().map(ArrowFieldVector::new).toList();
    var outputBatch = new RecordBatch(schema, cols);
    return Sequence.of(List.of(outputBatch).iterator());
  }
}
