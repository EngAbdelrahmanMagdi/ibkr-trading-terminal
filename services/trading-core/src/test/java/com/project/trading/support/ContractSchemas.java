package com.project.trading.support;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Validates API responses against the contract JSON Schemas (format assertions on, no remote retrieval). */
public final class ContractSchemas {

    private static final String BASE_ID = "https://contracts.trading-terminal.invalid/schemas/";
    private static final Path SCHEMAS = Path.of(System.getProperty("contracts.dir", "../../contracts"))
            .resolve("schemas").toAbsolutePath().normalize();
    private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
            builder -> builder
                    .schemaRegistryConfig(SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build())
                    .schemas(sources()));

    private ContractSchemas() {
    }

    private static Map<String, String> sources() {
        Map<String, String> sources = new LinkedHashMap<>();
        try (Stream<Path> files = Files.walk(SCHEMAS)) {
            for (Path p : files.filter(f -> f.toString().endsWith(".schema.json")).toList()) {
                String rel = SCHEMAS.relativize(p).toString().replace('\\', '/');
                sources.put(BASE_ID + rel, Files.readString(p, StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertThat(sources).as("contract schemas under " + SCHEMAS).isNotEmpty();
        return sources;
    }

    /** Asserts that a document conforms to the schema at the relative path (for example trading/order.schema.json). */
    public static void assertValid(String schema, JsonNode document) {
        Schema s = REGISTRY.getSchema(SchemaLocation.of(BASE_ID + schema));
        List<?> errors = s.validate(document.toString(), InputFormat.JSON);
        assertThat(errors).as(schema + " violations in " + document).isEmpty();
    }

    /** Asserts that every element of an array conforms to the item schema. */
    public static void assertEachValid(String schema, JsonNode array) {
        assertThat(array.isArray()).as("expected an array: " + array).isTrue();
        for (JsonNode item : array) {
            assertValid(schema, item);
        }
    }
}
