package dev.hienph.fusionj.executor.physical.expresisons;

import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.executor.physical.utils.Converts;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class LtEqExpression extends BooleanExpression {
    public LtEqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    boolean evaluate(Object l, Object r, ArrowType arrowType) {
        if (arrowType == ArrowTypes.Int8Type) {
            return (Byte) l <= (Byte) r;
        }
        if (arrowType == ArrowTypes.Int16Type) {
            return (Short) l <= (Short) r;
        }
        if (arrowType == ArrowTypes.Int32Type) {
            return (Integer) l <= (Integer) r;
        }
        if (arrowType == ArrowTypes.Int64Type) {
            return (Long) l <= (Long) r;
        }
        if (arrowType == ArrowTypes.FloatType) {
            return (Float) l <= (Float) r;
        }
        if (arrowType == ArrowTypes.DoubleType) {
            return (Double) l <= (Double) r;
        }
        if (arrowType == ArrowTypes.StringType) {
            return Converts.toString(l).compareTo(Converts.toString(r)) <= 0;
        }
        throw new IllegalStateException(String.format("Unsupported data type in comparison expression: %s", arrowType));
    }
}
