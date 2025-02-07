package dev.hienph.fusionj.logical.expression;

import dev.hienph.fusionj.datatypes.Field;

public interface LogicalExpr {

  Field toField(LogicalPlan input);
}
