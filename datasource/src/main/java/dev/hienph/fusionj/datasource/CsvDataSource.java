package dev.hienph.fusionj.executor.datasource;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import dev.hienph.fusionj.executor.datasource.utils.CsvAsSequence;
import dev.hienph.fusionj.datatypes.ArrowTypes;
import dev.hienph.fusionj.datatypes.Field;
import dev.hienph.fusionj.datatypes.RecordBatch;
import dev.hienph.fusionj.datatypes.Schema;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvDataSource implements DataSource {

  private static final Logger logger = LoggerFactory.getLogger(CsvDataSource.class);
  private final Boolean hasHeaders;
  private final Integer batchSize;
  private final Schema finalSchema;
  private final String filename;

  public CsvDataSource(
      String filename,
      Schema schema,
      Boolean hasHeaders,
      Integer batchSize
  ) {
    this.hasHeaders = hasHeaders;
    this.batchSize = batchSize;
    this.filename = filename;
    this.finalSchema = Optional.ofNullable(schema).orElseGet(this::inferSchema);
  }

  @Override
  public Sequence<RecordBatch> scan(List<String> projection) {
    logger.info("scan() projection = {}", projection.toString());
    var file = new File(filename);
    if (!file.exists()) {
      throw new RuntimeException(new FileNotFoundException(filename));
    }
    var readSchema = projection.isEmpty() ? finalSchema : finalSchema.select(projection);
    var settings = defaultSettings();
    if (!projection.isEmpty()) {
      settings.selectFields(projection.toArray(new String[0]));
    }
    settings.setHeaderExtractionEnabled(hasHeaders);
    if (!hasHeaders) {
      settings.setHeaders(
          readSchema.fields().stream().map(Field::name).toList().toArray(new String[0]));
    }
    var parser = buildParser(settings);
    parser.beginParsing(file);
    parser.getDetectedFormat();
    return new CsvAsSequence(readSchema, parser, batchSize);
  }

  @Override
  public Schema schema() {
    return finalSchema;
  }

  private CsvParser buildParser(CsvParserSettings settings) {
    return new CsvParser(settings);
  }

  private CsvParserSettings defaultSettings() {
    var p = new CsvParserSettings();
    p.setDelimiterDetectionEnabled(true);
    p.setLineSeparatorDetectionEnabled(true);
    p.setSkipEmptyLines(true);
    p.setAutoClosingEnabled(true);
    return p;
  }

  private Schema inferSchema() {
    logger.info("start -> inferSchema()");
    var file = new File(this.filename);
    if (!file.exists()) {
      throw new RuntimeException(new FileNotFoundException(file.getAbsolutePath()));
    }

    var parser = buildParser(defaultSettings());
    try {
      var stream = new FileInputStream(file);
      parser.beginParsing(stream);
      parser.getDetectedFormat();
      parser.parseNext();
      var headers = Arrays.stream(parser.getContext().parsedHeaders()).filter(Objects::nonNull);
      var schema = hasHeaders ? new Schema(
          headers.map(col -> new Field(col, ArrowTypes.StringType)).toList())
          : new Schema(
              IntStream.range(0, headers.toList().size())
                  .mapToObj(
                      i -> new Field("field_" + (i + 1), ArrowTypes.StringType))
                  .toList());
      parser.stopParsing();
      logger.info("done -> inferSchema()");
      return schema;
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
