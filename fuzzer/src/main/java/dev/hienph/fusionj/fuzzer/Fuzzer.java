package dev.hienph.fusionj.fuzzer;

import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import dev.hienph.fusionj.logical.And;
import dev.hienph.fusionj.logical.ColumnIndex;
import dev.hienph.fusionj.logical.DataFrame;
import dev.hienph.fusionj.logical.Eq;
import dev.hienph.fusionj.logical.Gt;
import dev.hienph.fusionj.logical.GtEq;
import dev.hienph.fusionj.logical.LiteralDouble;
import dev.hienph.fusionj.logical.LiteralLong;
import dev.hienph.fusionj.logical.LiteralString;
import dev.hienph.fusionj.logical.LogicalExpr;
import dev.hienph.fusionj.logical.Lt;
import dev.hienph.fusionj.logical.LtEq;
import dev.hienph.fusionj.logical.Neq;
import dev.hienph.fusionj.logical.Or;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Fuzzer {

    private final Random rand = new Random(0);
    private final EnhancedRandom enhancedRandom = new EnhancedRandom(rand);

    /**
     * Create a list of random values based on the provided data type
     */
    public List<Object> createValues(ArrowType arrowType, int n) {
        List<Object> values = new ArrayList<>();
        if (arrowType == ArrowTypes.Int8Type) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextByte());
            }
        } else if (arrowType == ArrowTypes.Int16Type) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextShort());
            }
        } else if (arrowType == ArrowTypes.Int32Type) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextInt());
            }
        } else if (arrowType == ArrowTypes.Int64Type) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextLong());
            }
        } else if (arrowType == ArrowTypes.FloatType) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextFloat());
            }
        } else if (arrowType == ArrowTypes.DoubleType) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextDouble());
            }
        } else if (arrowType == ArrowTypes.StringType) {
            for (int i = 0; i < n; i++) {
                values.add(enhancedRandom.nextString(rand.nextInt(64)));
            }
        } else {
            throw new IllegalStateException();
        }
        return values;
    }

    /**
     * Create a RecordBatch containing random data based on the provided schema
     */
    public RecordBatch createRecordBatch(Schema schema, int n) {
        List<List<Object>> columns = new ArrayList<>();
        for (Field field : schema.fields()) {
            columns.add(createValues(field.dataType(), n));
        }
        return createRecordBatch(schema, Collections.singletonList(columns));
    }

    /**
     * Create a RecordBatch containing the specified values.
     */
    public RecordBatch createRecordBatch(Schema schema, List<List<?>> columns) {
        int rowCount = columns.get(0).size();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();

        for (int row = 0; row < rowCount; row++) {
            for (int col = 0; col < columns.size(); col++) {
                var v = root.getVector(col);
                Object value = columns.get(col).get(row);
                if (v instanceof BitVector) {
                    ((BitVector) v).set(row, (Boolean) value ? 1 : 0);
                } else if (v instanceof TinyIntVector) {
                    ((TinyIntVector) v).set(row, (Byte) value);
                } else if (v instanceof SmallIntVector) {
                    ((SmallIntVector) v).set(row, (Short) value);
                } else if (v instanceof IntVector) {
                    ((IntVector) v).set(row, (Integer) value);
                } else if (v instanceof BigIntVector) {
                    ((BigIntVector) v).set(row, (Long) value);
                } else if (v instanceof Float4Vector) {
                    ((Float4Vector) v).set(row, (Float) value);
                } else if (v instanceof Float8Vector) {
                    ((Float8Vector) v).set(row, (Double) value);
                } else if (v instanceof VarCharVector) {
                    ((VarCharVector) v).set(row, ((String) value).getBytes());
                } else {
                    throw new IllegalStateException();
                }
            }
        }
        root.setRowCount(rowCount);

        return new RecordBatch(schema, root.getFieldVectors().stream().map(ArrowFieldVector::new).toList());
    }

    public DataFrame createPlan(DataFrame input, int depth, int maxDepth, int maxExprDepth) {
        if (depth == maxDepth) {
            return input;
        } else {
            DataFrame child = createPlan(input, depth + 1, maxDepth, maxExprDepth);
            switch (rand.nextInt(2)) {
                case 0:
                    int exprCount = 1 + rand.nextInt(4);
                    return child.project(IntStream.range(0, exprCount).mapToObj(i -> createExpression(child, 0, maxExprDepth)).collect(Collectors.toList()));
                case 1:
                    return child.filter(createExpression(input, 0, maxExprDepth));
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public LogicalExpr createExpression(DataFrame input, int depth, int maxDepth) {
        if (depth == maxDepth) {
            // return a leaf node
            switch (rand.nextInt(4)) {
                case 0:
                    return new ColumnIndex(rand.nextInt(input.schema().fields().size()));
                case 1:
                    return new LiteralDouble(enhancedRandom.nextDouble());
                case 2:
                    return new LiteralLong(enhancedRandom.nextLong());
                case 3:
                    return new LiteralString(enhancedRandom.nextString(rand.nextInt(64)));
                default:
                    throw new IllegalStateException();
            }
        } else {
            // binary expressions
            LogicalExpr l = createExpression(input, depth + 1, maxDepth);
            LogicalExpr r = createExpression(input, depth + 1, maxDepth);
            switch (rand.nextInt(8)) {
                case 0:
                    return new Eq(l, r);
                case 1:
                    return new Neq(l, r);
                case 2:
                    return new Lt(l, r);
                case 3:
                    return new LtEq(l, r);
                case 4:
                    return new Gt(l, r);
                case 5:
                    return new GtEq(l, r);
                case 6:
                    return new And(l, r);
                case 7:
                    return new Or(l, r);
                default:
                    throw new IllegalStateException();
            }
        }
    }
}

