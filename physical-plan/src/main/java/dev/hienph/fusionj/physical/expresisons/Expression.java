package dev.hienph.fusionj.executor.physical.expresisons;

import dev.hienph.fusionj.datatypes.ColumnVector;
import dev.hienph.fusionj.datatypes.RecordBatch;

public interface Expression {

    /*
    Evaluate the expression against an input record batch
    and produce a column of data as output
     */
    ColumnVector evaluate(RecordBatch input);
}
