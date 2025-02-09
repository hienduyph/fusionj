package dev.hienph.fusionj.physical;

public record Task(
        String jobUuid,
        Integer stageId,
        Integer taskId,
        Integer partitionId,
        PhysicalPlan plan) {
}
