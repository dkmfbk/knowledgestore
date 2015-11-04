package eu.fbk.knowledgestore.data;

import org.junit.Test;

public class DataTest {

    @Test
    public void testCleanURI() {
        final String uri = "http://en.wikinews.org/wiki/CEO_of_GM_outlines_plan_for_"
                + "&quot;New_GM&quot;_after_auto_company_declared_bankruptcy";
        final String cleanedURI = Data.cleanURI(uri);
        System.out.println(cleanedURI + " -- " + cleanedURI.equals(uri));
    }

}
