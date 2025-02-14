package dev.hienph.fusionj.sql;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum Keyword implements TokenType {
  SCHEMA,
  DATABASE,
  TABLE,
  COLUMN,
  VIEW,
  INDEX,
  TRIGGER,
  PROCEDURE,
  TABLESPACE,
  FUNCTION,
  SEQUENCE,
  CURSOR,
  FROM,
  TO,
  OF,
  IF,
  ON,
  FOR,
  WHILE,
  DO,
  NO,
  BY,
  WITH,
  WITHOUT,
  TRUE,
  FALSE,
  TEMPORARY,
  TEMP,
  COMMENT,

  /**
   * create
   */
  CREATE,
  REPLACE,
  BEFORE,
  AFTER,
  INSTEAD,
  EACH,
  ROW,
  STATEMENT,
  EXECUTE,
  BITMAP,
  NOSORT,
  REVERSE,
  COMPILE,

  /**
   * alter
   */
  ALTER,
  ADD,
  MODIFY,
  RENAME,
  ENABLE,
  DISABLE,
  VALIDATE,
  USER,
  IDENTIFIED,

  /**
   * truncate
   */
  TRUNCATE,

  /**
   * drop
   */
  DROP,
  CASCADE,

  /**
   * insert
   */
  INSERT,
  INTO,
  VALUES,

  /**
   * update
   */
  UPDATE,
  SET,

  /**
   * delete
   */
  DELETE,

  /**
   * select
   */
  SELECT,
  DISTINCT,
  AS,
  CASE,
  WHEN,
  ELSE,
  THEN,
  END,
  LEFT,
  RIGHT,
  FULL,
  INNER,
  OUTER,
  CROSS,
  JOIN,
  USE,
  USING,
  NATURAL,
  WHERE,
  ORDER,
  ASC,
  DESC,
  GROUP,
  HAVING,
  UNION,

  /**
   * others
   */
  DECLARE,
  GRANT,
  FETCH,
  REVOKE,
  CLOSE,
  CAST,
  NEW,
  ESCAPE,
  LOCK,
  SOME,
  LEAVE,
  ITERATE,
  REPEAT,
  UNTIL,
  OPEN,
  OUT,
  INOUT,
  OVER,
  ADVISE,
  SIBLINGS,
  LOOP,
  EXPLAIN,
  DEFAULT,
  EXCEPT,
  INTERSECT,
  MINUS,
  PASSWORD,
  LOCAL,
  GLOBAL,
  STORAGE,
  DATA,
  COALESCE,

  /**
   * Types
   */
  CHAR,
  CHARACTER,
  VARYING,
  VARCHAR,
  VARCHAR2,
  INTEGER,
  INT,
  SMALLINT,
  DECIMAL,
  DEC,
  NUMERIC,
  FLOAT,
  REAL,
  DOUBLE,
  PRECISION,
  DATE,
  TIME,
  INTERVAL,
  BOOLEAN,
  BLOB,

  /**
   * Conditionals
   */
  AND,
  OR,
  XOR,
  IS,
  NOT,
  NULL,
  IN,
  BETWEEN,
  LIKE,
  ANY,
  ALL,
  EXISTS,

  /**
   * Functions
   */
  AVG,
  MAX,
  MIN,
  SUM,
  COUNT,
  GREATEST,
  LEAST,
  ROUND,
  TRUNC,
  POSITION,
  EXTRACT,
  LENGTH,
  CHAR_LENGTH,
  SUBSTRING,
  SUBSTR,
  INSTR,
  INITCAP,
  UPPER,
  LOWER,
  TRIM,
  LTRIM,
  RTRIM,
  BOTH,
  LEADING,
  TRAILING,
  TRANSLATE,
  CONVERT,
  LPAD,
  RPAD,
  DECODE,
  NVL,

  /**
   * Constraints
   */
  CONSTRAINT,
  UNIQUE,
  PRIMARY,
  FOREIGN,
  KEY,
  CHECK,
  REFERENCES;

  private static final Map<String, Keyword> keywords = Arrays.stream(values())
    .collect(Collectors.toMap(Keyword::name, Function.identity()));

  public static Keyword textOf(String text) {
    return keywords.get(text);
  }
}
