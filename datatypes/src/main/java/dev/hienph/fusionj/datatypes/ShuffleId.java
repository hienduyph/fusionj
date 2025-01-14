package dev.hienph.fusionj.datatypes;

/*
message ShuffleId {
  string job_uuid = 1;
  uint32 stage_id = 2;
  uint32 partition_id = 4;
}
 */
public record ShuffleId(
    String jobUuid,
    Integer  stageId,
    Integer partitionId
) {
}
