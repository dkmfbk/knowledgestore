package eu.fbk.knowledgestore.triplestore;

import java.util.Set;

import junit.framework.Test;

import com.google.common.collect.ImmutableSet;

import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.manifest.SPARQL11ManifestTest;
import org.openrdf.query.parser.sparql.manifest.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

public class SPARQLRendererW3CTest extends SPARQLQueryTest {

    private static final Set<String> EXCLUDED_TESTS = ImmutableSet.of("Error in AVG",
            "Protect from error in AVG", "MD5() over Unicode data", "SHA1() on Unicode data",
            "SHA256() on Unicode data", "SHA512() on Unicode data",
            "sq03 - Subquery within graph pattern, graph variable is not bound");

    public static Test suite() throws Exception {
        return SPARQL11ManifestTest.suite(
                new Factory() {

                    @Override
                    public SPARQLRendererW3CTest createSPARQLQueryTest(final String testURI,
                            final String name, final String queryFileURL,
                            final String resultFileURL, final Dataset dataSet,
                            final boolean laxCardinality) {
                        return createSPARQLQueryTest(testURI, name, queryFileURL, resultFileURL,
                                dataSet, laxCardinality, false);
                    }

                    @Override
                    public SPARQLRendererW3CTest createSPARQLQueryTest(final String testURI,
                            final String name, final String queryFileURL,
                            final String resultFileURL, final Dataset dataSet,
                            final boolean laxCardinality, final boolean checkOrder) {
                        for (final String excluded : EXCLUDED_TESTS) {
                            if (name.contains(excluded)) {
                                return null;
                            }
                        }
                        return new SPARQLRendererW3CTest(testURI, name, queryFileURL,
                                resultFileURL, dataSet, laxCardinality, checkOrder);
                    }
                }, true, true, false, "service", "add", "basic-update", "clear", "copy",
                "delete-data",
                "delete-insert", "delete-where", "delete", "drop", "entailment", "json-res",
                "move", "syntax-query", "syntax-update-1", "syntax-update-2", "update-silent",
                "syntax-fed", "service-description", "protocol", "http-rdf-update");
    }

    private SPARQLRendererW3CTest(final String testURI, final String name,
            final String queryFileURL, final String resultFileURL, final Dataset dataSet,
            final boolean laxCardinality, final boolean checkOrder) {
        super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder);
    }

    @Override
    protected Repository newRepository() {
        // return new DatasetRepository(new SailRepository(new MemoryStore()));
        return new DatasetRepository(new SailRepository(new SPARQLRendererSail(new MemoryStore())));
    }

    @Override
    protected void runTest() throws Exception {
        System.out.println("### " + getName() + " ###");
        super.runTest();
    }

}
