package dev.hienph.fusionj.physical;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;
import dev.hienph.fusionj.datatypes.Schema;
import dev.hienph.fusionj.fuzzer.Fuzzer;
import dev.hienph.fusionj.physical.expresisons.ColumnExpression;
import dev.hienph.fusionj.physical.expresisons.GtEqExpression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

public class BooleanExpressionTest {
    @Test
    void gteqBytes() {
        var schema = new Schema(List.of(new Field("a", ArrowTypes.Int8Type),
                new Field("b", ArrowTypes.Int8Type)));
        List<Byte> a = List.of((byte) 10, (byte) 20, (byte) 30, Byte.MIN_VALUE, Byte.MAX_VALUE);
        List<Byte> b = List.of((byte) 10, (byte) 30, (byte) 20, Byte.MAX_VALUE, Byte.MIN_VALUE);
        var batch = new Fuzzer().createRecordBatch(schema, List.of(a, b));
        var expr = new GtEqExpression(new ColumnExpression(0), new ColumnExpression(1));
        var result = expr.evaluate(batch);
        System.out.println(result.toString());
        Assertions.assertEquals(a.size(), result.size());
        IntStream.range(0, result.size()).forEach(it -> {
            Assertions.assertEquals(a.get(it) >= b.get(it), result.getValue(it));
        });
    }
}
