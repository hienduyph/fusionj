package dev.hienph.fusionj.physical.expresisons;

import dev.hienph.fusionj.physical.utils.Converts;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class OrExpression extends BooleanExpression {
    public OrExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    boolean evaluate(Object l, Object r, ArrowType arrowType) {
        return Converts.toBool(l) || Converts.toBool(r);
    }
}
