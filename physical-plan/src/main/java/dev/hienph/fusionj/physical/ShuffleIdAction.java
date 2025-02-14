package dev.hienph.fusionj.executor.physical;

import dev.hienph.fusionj.datatypes.ShuffleId;

public record ShuffleIdAction(ShuffleId shuffleId) implements Action {
}
