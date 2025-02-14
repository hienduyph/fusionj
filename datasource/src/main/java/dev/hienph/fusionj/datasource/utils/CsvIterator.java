package dev.hienph.fusionj.executor.datasource.utils;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import dev.hienph.fusionj.datatypes.ArrowFieldVector;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvIterator implements Iterator<RecordBatch> {

  private static final Logger logger = LoggerFactory.getLogger(CsvIterator.class);

  private final Schema schema;
  private final CsvParser parser;
  private final Integer batchSize;
  private RecordBatch next;
  private Boolean started = false;

  public CsvIterator(
      Schema schema,
      CsvParser parser,
      Integer batchSize
  ) {
    this.schema = schema;
    this.parser = parser;
    this.batchSize = batchSize;
  }

  @Override
  public boolean hasNext() {
    if (!started) {
      started = true;
      next = nextBatch();
    }
    return next != null;
  }

  @Override
  public RecordBatch next() {
    if (!started) {
      hasNext();
    }
    var out = next;
    next = nextBatch();
    if (out == null) {
      throw new NoSuchElementException(
          "can not read past the end of " + CsvIterator.class.getSimpleName());
    }
    return out;
  }

  private RecordBatch nextBatch() {
    var rows = new ArrayList<Record>(batchSize);
    Record line;
    do {
      line = parser.parseNextRecord();
      if (line != null) {
        rows.add(line);
      }
    } while (line != null && rows.size() < batchSize);
    if (rows.isEmpty()) {
      return null;
    }
    return createBatch(rows);
  }

  private RecordBatch createBatch(ArrayList<Record> rows) {
    var root = VectorSchemaRoot.create(schema.toArrow(), new RootAllocator(Long.MAX_VALUE));
    root.getFieldVectors().forEach(it -> it.setInitialCapacity(rows.size()));
    root.allocateNew();
    IntStream.range(0, root.getFieldVectors().size()).forEach(fieldIdx -> {
      var vector = root.getFieldVectors().get(fieldIdx);
      switch (vector) {
        case VarCharVector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var row = rows.get(rowIdx);
            var valueStr = row.getValue(vector.getName(), "").trim();
            b.setSafe(rowIdx, valueStr.getBytes());
          });
        }
        case TinyIntVector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Byte.parseByte(valueStr));
            }
          });
        }
        case SmallIntVector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Short.parseShort(valueStr));
            }
          });
        }
        case IntVector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Integer.parseInt(valueStr));
            }
          });
        }
        case BigIntVector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Long.parseLong(valueStr));
            }
          });
        }
        case Float4Vector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Float.parseFloat(valueStr));
            }
          });
        }
        case Float8Vector b -> {
          IntStream.range(0, rows.size()).forEach(rowIdx -> {
            var valueStr = rows.get(rowIdx).getValue(vector.getName(), "").trim();
            if (valueStr.isEmpty()) {
              b.setNull(rowIdx);
            } else {
              b.set(rowIdx, Double.parseDouble(valueStr));
            }
          });
        }
        default -> throw new IllegalStateException(
            "No support for reading CSV columns with data type " + vector.toString());
      }
      vector.setValueCount(rows.size());
    });
    return new RecordBatch(schema,
        root.getFieldVectors().stream().map(ArrowFieldVector::new).toList());
  }
}
