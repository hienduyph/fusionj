package dev.hienph.fusionj.physical.expresisons;

import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Objects;

public class EqExpression extends BooleanExpression {
    public EqExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    boolean evaluate(Object l, Object r, ArrowType arrowType) {
        return Objects.equals(l, r);
    }
}
