package dev.hienph.fusionj.datatypes;

public record ShuffleLocation(
    String jobUuid,
    Integer stageId,
    Integer partitionId,
    String executionUuid
) {
}
