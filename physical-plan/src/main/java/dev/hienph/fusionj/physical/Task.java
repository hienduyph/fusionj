package dev.hienph.fusionj.executor.physical;

public record Task(
        String jobUuid,
        Integer stageId,
        Integer taskId,
        Integer partitionId,
        PhysicalPlan plan) {
}
