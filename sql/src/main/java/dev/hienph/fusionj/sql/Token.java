package dev.hienph.fusionj.sql;

public record Token(String text, TokenType type, Integer endOffset) {

  @Override
  public String toString() {
    final var tokenType = switch (type) {
      case Keyword k -> "Keyword";
      case Symbol b -> "Symbol";
      case Literal t -> "Literal";
      default -> "";
    };
    return String.format("Token(\"%s\", %s.%s, %s", text, tokenType, type, endOffset);
  }
}
