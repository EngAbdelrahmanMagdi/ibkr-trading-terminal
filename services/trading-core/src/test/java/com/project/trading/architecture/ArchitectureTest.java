package com.project.trading.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/** Layering and module boundaries. */
class ArchitectureTest {

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.project.trading");

    @Test
    void domainIsFrameworkFree() {
        noClasses().that().resideInAPackage("..domain..")
                .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta.persistence..",
                        "org.hibernate..", "tools.jackson..", "com.fasterxml..", "io.micrometer..",
                        "..infrastructure..", "..application..", "..api..")
                .check(CLASSES);
    }

    @Test
    void modulesDoNotReachIntoEachOthersInfrastructureOrApi() {
        for (String module : new String[]{"instrument", "order", "execution", "position", "portfolio", "watchlist",
                "broker", "marketdata", "outbox", "inbox"}) {
            noClasses().that().resideOutsideOfPackage("com.project.trading." + module + "..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "com.project.trading." + module + ".infrastructure..",
                            "com.project.trading." + module + ".api..")
                    .because("modules interact through application services and domain ports only")
                    .check(CLASSES);
        }
    }

    @Test
    void controllersContainNoPersistenceOrBrokerAccess() {
        noClasses().that().resideInAPackage("..api..")
                .should().dependOnClassesThat().resideInAnyPackage("jakarta.persistence..", "org.springframework.data..",
                        "com.project.trading.broker.domain..")
                .check(CLASSES);
    }

    @Test
    void theDomainAndApplicationNeverSeeTheSimulatedBroker() {
        noClasses().that().resideInAnyPackage("..domain..", "..application..", "..api..")
                .should().dependOnClassesThat().resideInAPackage("com.project.trading.broker.infrastructure..")
                .check(CLASSES);
    }

    @Test
    void modulesHaveNoCycles() {
        slices().matching("com.project.trading.(*)..").should().beFreeOfCycles().check(CLASSES);
    }
}
