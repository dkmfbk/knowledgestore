package eu.fbk.knowledgestore.server.http;

import javax.ws.rs.core.Application;

import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;

public class RepresentationResourceTest extends JerseyTest {

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return null; // TODO
    }

}
