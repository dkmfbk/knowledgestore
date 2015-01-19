package eu.fbk.knowledgestore.triplestore;

import junit.framework.Test;

import org.openrdf.query.Dataset;
import org.openrdf.query.parser.sparql.manifest.ManifestTest;
import org.openrdf.query.parser.sparql.manifest.SPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.dataset.DatasetRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

public class SPARQLRendererDAWGTest extends SPARQLQueryTest {

    public static Test suite() throws Exception {
        return ManifestTest.suite(new Factory() {

            @Override
            public SPARQLRendererDAWGTest createSPARQLQueryTest(final String testURI,
                    final String name, final String queryFileURL, final String resultFileURL,
                    final Dataset dataSet, final boolean laxCardinality) {
                return createSPARQLQueryTest(testURI, name, queryFileURL, resultFileURL, dataSet,
                        laxCardinality, false);
            }

            @Override
            public SPARQLRendererDAWGTest createSPARQLQueryTest(final String testURI,
                    final String name, final String queryFileURL, final String resultFileURL,
                    final Dataset dataSet, final boolean laxCardinality, final boolean checkOrder) {
                return new SPARQLRendererDAWGTest(testURI, name, queryFileURL, resultFileURL,
                        dataSet, laxCardinality, checkOrder);
            }
        });
    }

    private SPARQLRendererDAWGTest(final String testURI, final String name,
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
