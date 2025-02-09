package dev.hienph.fusionj.physical;

import dev.hienph.fusionj.datasource.Sequence;
import dev.hienph.fusionj.datatypes.*;
import dev.hienph.fusionj.physical.expresisons.Expression;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public record SelectionExec(PhysicalPlan input, Expression expr) implements PhysicalPlan {
    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(input);
    }

    @Override
    public Sequence<RecordBatch> execute() {
        var ss = Sequence.stream(input.execute().iterator());
        var r = ss.map(batch -> {
            var result = (BitVector) ((ArrowFieldVector) expr.evaluate(batch)).field();
            var schema = batch.schema();
            var columnCount = batch.schema().fields().size();
            var filterFields = IntStream.range(0, columnCount).mapToObj(it -> filter(batch.field(it), result));
            var fields = filterFields.map(ArrowFieldVector::new).toList();
            return new RecordBatch(schema, fields);
        });
        return Sequence.of(r.iterator());
    }

    private FieldVector filter(ColumnVector v, BitVector selection) {
        var filteredVector = new VarCharVector("v", new RootAllocator(Long.MAX_VALUE));
        filteredVector.allocateNew();
        var builder = new ArrowVectorBuilder(filteredVector);
        AtomicInteger count = new AtomicInteger();
        IntStream.range(0, selection.getValueCount()).forEach(it -> {
            if (selection.get(it) == 1) {
                builder.set(count.getAndIncrement(), v.getValue(it));
            }
        });
        filteredVector.setValueCount(count.intValue());
        return filteredVector;
    }
}
