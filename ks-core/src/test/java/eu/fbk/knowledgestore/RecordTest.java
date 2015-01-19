package eu.fbk.knowledgestore;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.DCTERMS;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.vocabulary.KS;
import eu.fbk.knowledgestore.vocabulary.NIF;

public class RecordTest {

    private static final URI BOOL_PROP = new URIImpl("test:boolProp");

    @Test
    public void test() {
        final Record r = Record.create();
        r.setID(Data.getValueFactory().createURI("test:r"));
        r.set(DCTERMS.TITLE, "this is the title");
        final Record r2 = Record.create();
        r2.setID(Data.getValueFactory().createURI("test:r2"));
        r2.set(DCTERMS.TITLE, "this is the title");
        final Record m = Record.create();
        m.setID(Data.getValueFactory().createURI("test:x"));
        m.set(NIF.END_INDEX, 15);
        m.set(RDFS.COMMENT, "first", "second", "third");
        m.set(KS.MENTION_OF, r, r2);
        System.out.println(m.toString(Data.getNamespaceMap(), true));
    }

    @Test
    public void testPropertyAccessors() {

        final Record r = Record.create();
        checkSet(r.getProperties());
        checkSet(r.get(RDF.TYPE));

        Assert.assertTrue(r.isNull(RDF.TYPE));
        Assert.assertTrue(r.isUnique(RDF.TYPE));
        Assert.assertFalse(r.isTrue(BOOL_PROP));
        Assert.assertFalse(r.isFalse(BOOL_PROP));

        r.set(RDF.TYPE, OWL.THING);
        checkSet(r.getProperties(), RDF.TYPE);
        checkSet(r.get(RDF.TYPE), OWL.THING);
        Assert.assertFalse(r.isNull(RDF.TYPE));
        Assert.assertTrue(r.isUnique(RDF.TYPE));
        try {
            r.isTrue(RDF.TYPE);
            Assert.fail();
        } catch (final IllegalArgumentException ex) {
            // ignore
        }
        try {
            r.isFalse(RDF.TYPE);
            Assert.fail();
        } catch (final IllegalArgumentException ex) {
            // ignore
        }

        r.add(RDF.TYPE, RDFS.RESOURCE);
        checkSet(r.getProperties(), RDF.TYPE);
        checkSet(r.get(RDF.TYPE), OWL.THING, RDFS.RESOURCE);
        Assert.assertFalse(r.isUnique(RDF.TYPE));

        r.remove(RDF.TYPE, RDFS.RESOURCE);
        checkSet(r.getProperties(), RDF.TYPE);
        checkSet(r.get(RDF.TYPE), OWL.THING);
        Assert.assertTrue(r.isUnique(RDF.TYPE));

        r.add(RDFS.LABEL, "label");
        checkSet(r.getProperties(), RDF.TYPE, RDFS.LABEL);

        r.remove(RDFS.LABEL, "label");
        checkSet(r.getProperties(), RDF.TYPE);

        r.set(RDF.TYPE, null);
        checkSet(r.getProperties());
        checkSet(r.get(RDF.TYPE));
        Assert.assertTrue(r.isNull(RDF.TYPE));

        r.set(RDF.TYPE, ImmutableList.of(OWL.THING));
        checkSet(r.getProperties(), RDF.TYPE);
        checkSet(r.get(RDF.TYPE), OWL.THING);

        r.set(RDF.TYPE, null);
        checkSet(r.getProperties());
        checkSet(r.get(RDF.TYPE));

        r.set(BOOL_PROP, true);
        Assert.assertTrue(r.isTrue(BOOL_PROP));
        Assert.assertFalse(r.isFalse(BOOL_PROP));

        r.set(BOOL_PROP, false);
        Assert.assertFalse(r.isTrue(BOOL_PROP));
        Assert.assertTrue(r.isFalse(BOOL_PROP));

        r.add(BOOL_PROP, true);
        try {
            r.isTrue(BOOL_PROP);
            Assert.fail();
        } catch (final IllegalStateException ex) {
            // ignore
        }
        try {
            r.isFalse(BOOL_PROP);
            Assert.fail();
        } catch (final IllegalStateException ex) {
            // ignore
        }
    }

    private void checkSet(final Iterable<?> nodes, final Object... expected) {
        final Set<Object> nodeSet = Sets.newHashSet(nodes);
        Assert.assertEquals(expected.length, nodeSet.size());
        for (final Object element : expected) {
            Assert.assertTrue(nodeSet.contains(element));
        }
    }

}
