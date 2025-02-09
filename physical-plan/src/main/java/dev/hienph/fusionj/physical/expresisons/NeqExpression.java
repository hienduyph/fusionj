package dev.hienph.fusionj.physical.expresisons;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Objects;

public class NeqExpression extends BooleanExpression {
    public NeqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    boolean evaluate(Object l, Object r, ArrowType arrowType) {
        return !Objects.equals(l, r);
    }
}
