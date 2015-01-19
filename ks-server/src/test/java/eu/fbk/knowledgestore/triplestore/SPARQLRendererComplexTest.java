package eu.fbk.knowledgestore.triplestore;

import org.openrdf.query.parser.sparql.ComplexSPARQLQueryTest;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

public class SPARQLRendererComplexTest extends ComplexSPARQLQueryTest {

    @Override
    protected Repository newRepository() throws Exception {
        return new SailRepository(new SPARQLRendererSail(new MemoryStore()));
    }

}
