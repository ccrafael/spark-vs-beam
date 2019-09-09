package org.rcc.utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BuildInput {
    private final static Path INPUTFILE = Paths.get("input.txt");

    public static void main(String args[]) throws IOException {

        if (Files.exists(INPUTFILE)) {
            Files.delete(INPUTFILE);
        }

        String[] words = new String[]{"foo", "var", "foovar", "varfoo"};

        Arrays.stream(words).forEach(BuildInput::writeLines);

    }

    private static void writeLines(String word) {
        IntStream.range(0, 1_000_000).forEach(i -> writeOneLine(word));
    }

    private static void writeOneLine(String word) {
        try {

            Files.write(INPUTFILE,
                    Arrays.asList(IntStream.range(0, 100).mapToObj(i -> word).collect(Collectors.joining(" "))),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
