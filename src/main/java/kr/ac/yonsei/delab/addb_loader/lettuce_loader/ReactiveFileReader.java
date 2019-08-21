package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import lombok.Getter;
import lombok.Setter;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.BaseStream;

public class ReactiveFileReader {
    @Getter @Setter private Path path;

    public ReactiveFileReader(Path path) {
        this.path = path;
    }

    public Flux<String> parse() {
        return Flux.using(
                () -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }
}
