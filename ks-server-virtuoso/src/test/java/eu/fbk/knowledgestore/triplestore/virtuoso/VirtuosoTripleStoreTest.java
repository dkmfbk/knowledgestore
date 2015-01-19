package eu.fbk.knowledgestore.triplestore.virtuoso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Stream;
import eu.fbk.knowledgestore.runtime.Files;
import eu.fbk.knowledgestore.triplestore.SelectQuery;
import eu.fbk.knowledgestore.triplestore.TripleTransaction;

/**
 * Test case for {@link VirtuosoTripleStore}.
 *
 * @author Michele Mostarda (mostarda@fbk.eu)
 */
public class VirtuosoTripleStoreTest {

    private static final int CURSOR_STATEMENT_COUNT = 1000;

    private static final int BULK_STATEMENT_COUNT = 100 * 1000;

    private static final int BULK_MULTICONTEXT_STATEMENT_COUNT = 10 * 1000;

    private static final int BULK_MULTICONTEXT_CONTEXT_SIZE = 1000;

    private static final int MASSIVE_BULK_STATEMENT_COUNT = 10 * 1000 * 1000;

    private static final int MASSIVE_BULK_CONTEXT_COUNT = 100000;

    private static final int NUM_OF_DEFAULT_STATEMENTS = 2617;

    private static final Logger LOG = LoggerFactory.getLogger(VirtuosoTripleStoreTest.class);

    private VirtuosoTripleStore store;

    /**
     * Test initialization.
     *
     * @throws IOException
     *             on failure
     */
    @Before
    public void setUp() throws IOException {
        final FileSystem fileSystem = Files.getFileSystem("file:///${java.io.tmpdir}/virtuoso",
                ImmutableMap.of("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem"));
        this.store = new VirtuosoTripleStore(fileSystem, "localhost", 1111, "dba", "dba", false,
                5000, 200, "virtuoso.bulk.transaction");
        this.store.init();
        this.store.reset(); // Cleanup database.
    }

    /**
     * Test cleanup.
     *
     * @throws IOException
     *             on failure
     */
    @After
    public void tearDown() throws IOException {
        this.store.close();
        this.store = null;
    }

    /**
     * Test for {@link VirtuosoTripleStore#getVirtuoso()}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testGetVirtuoso() throws IOException {
        assertNotNull(this.store.getVirtuoso());
    }

    /**
     * Test for {@link VirtuosoTripleStore#reset()}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testReset() throws IOException {
        this.store.reset();
        assertEquals(NUM_OF_DEFAULT_STATEMENTS, countStatementsInStore(null));
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#get(Resource, URI, Value, Resource)}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testGet() throws IOException {
        final VirtuosoTripleTransaction tripleTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement = createFakeStatement();
        tripleTransaction.add(testStatement);
        final CloseableIteration<? extends Statement, ? extends Exception> iteration;
        iteration = tripleTransaction.get(testStatement.getSubject(),
                testStatement.getPredicate(), testStatement.getObject(),
                testStatement.getContext());
        final Stream<Statement> stream = Stream.create(iteration);
        try {
            final Iterator<? extends Statement> iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            final Statement matched = iterator.next();
            assertEquals(testStatement, matched);
            assertFalse(iterator.hasNext());
        } finally {
            stream.close();
        }
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#add(Statement)}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAdd() throws IOException {
        final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement = createFakeStatement();
        transaction.add(testStatement);
        checkStatementExists(transaction, testStatement, true);
        transaction.end(true);

        // Post condition: transaction persistence.
        final TripleTransaction checkTransaction = this.store.begin(true);
        checkStatementExists(checkTransaction, testStatement, true);
        checkTransaction.end(false);
    }

    /**
     * Test for add - rollback transaction work.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAddRollback() throws IOException {
        final VirtuosoTripleTransaction tripleTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement = createFakeStatement();
        tripleTransaction.add(testStatement);
        checkStatementExists(tripleTransaction, testStatement, true);
        tripleTransaction.end(false);

        // Post condition: transaction persistence.
        final TripleTransaction checkTransaction = this.store.begin(true);
        checkStatementExists(checkTransaction, testStatement, false);
        checkTransaction.end(false);
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#addBulk(Iterable, boolean)} with statements
     * belonging to the same context.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAddBulk() throws IOException {
        final Iterable<Statement> statements = createFakeStatements(BULK_STATEMENT_COUNT);
        final int triplesBefore = countStatementsInStore(null);
        final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final long beginTime = System.currentTimeMillis();
        transaction.addBulk(statements, false);
        transaction.end(true);
        final long endTime = System.currentTimeMillis();
        final long elapsed = endTime - beginTime;
        LOG.debug("Added statements: {}, Elapsed time: {} ms, statements/ms: {}\n",
                BULK_STATEMENT_COUNT, elapsed, BULK_STATEMENT_COUNT / (float) elapsed);
        final int triplesAfter = countStatementsInStore(null);
        assertEquals(BULK_STATEMENT_COUNT, triplesAfter - triplesBefore);
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#addBulk(Iterable, boolean)} with added statements
     * belonging to multiple contexts.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAddBulkDifferentContexts() throws IOException {
        for (int i = 1; i <= BULK_MULTICONTEXT_STATEMENT_COUNT //
                / BULK_MULTICONTEXT_CONTEXT_SIZE; i++) {
            final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                    .begin(false);
            try {
                final long beginTime = System.currentTimeMillis();
                final int contextCount = BULK_MULTICONTEXT_CONTEXT_SIZE * i;
                transaction.addBulk(
                        createFakeStatements(BULK_MULTICONTEXT_STATEMENT_COUNT, contextCount),
                        false);
                final long endTime = System.currentTimeMillis();
                System.out.printf("Added statements: %d, Contexts: %d, statements/context: "
                        + "%f, elapsed time: %d, statements/ms: %f\n",
                        BULK_MULTICONTEXT_STATEMENT_COUNT, contextCount,
                        BULK_MULTICONTEXT_STATEMENT_COUNT / (float) contextCount, endTime
                                - beginTime, BULK_MULTICONTEXT_STATEMENT_COUNT
                                / (float) (endTime - beginTime));
            } finally {
                transaction.end(true);
            }
        }
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#addBulk(Iterable, boolean)}.
     *
     * @throws IOException
     *             on failure
     */
    @Ignore
    @Test
    public void testAddMassive() throws IOException {
        final Iterable<Statement> statements = createFakeStatements(MASSIVE_BULK_STATEMENT_COUNT,
                MASSIVE_BULK_CONTEXT_COUNT);
        final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final long beginTime = System.currentTimeMillis();
        transaction.addBulk(statements, false);
        transaction.end(true);
        final long endTime = System.currentTimeMillis();
        final long elapsed = endTime - beginTime;
        LOG.debug("Added statements: {}, Elapsed time: {} ms, statements/ms: {}\n",
                MASSIVE_BULK_STATEMENT_COUNT, elapsed, MASSIVE_BULK_STATEMENT_COUNT
                        / (float) elapsed);
    }

    /**
     * Test for {@link TripleTransaction#add(eu.fbk.knowledgestore.Cursor)}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAddByCursor() throws IOException {
        final int addedTriples = CURSOR_STATEMENT_COUNT;
        final Iterable<Statement> statements = createFakeStatements(addedTriples);
        final int triplesBefore = countStatementsInStore(null);
        final TripleTransaction transaction = this.store.begin(false);
        transaction.add(Stream.create(statements));
        transaction.end(true);
        final int triplesAfter = countStatementsInStore(null);
        assertEquals(addedTriples, triplesAfter - triplesBefore);
    }

    /**
     * Test for addbulk - rollback transaction work.
     *
     * @throws IOException
     *             on failure
     */
    @Ignore
    @Test
    public void testAddBulkRollback() throws IOException {
        final VirtuosoTripleTransaction tripleTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement[] testStatements = new Statement[] { createFakeStatement(),
                createFakeStatement(), createFakeStatement(), createFakeStatement() };
        tripleTransaction.addBulk(Arrays.asList(testStatements), true);
        for (final Statement testStatement : testStatements) {
            checkStatementExists(tripleTransaction, testStatement, true);
        }
        tripleTransaction.end(false);

        // Post condition: transaction persistence.
        final TripleTransaction checkTransaction = this.store.begin(true);
        for (final Statement testStatement : testStatements) {
            checkStatementExists(checkTransaction, testStatement, false);
        }
        checkTransaction.end(false);
    }

    /**
     * Test to verify the behavior of the driver when flushing the internal buffer in transaction
     * mode.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testAddBufferOverflow() throws IOException {
        this.store.getVirtuoso().setBatchSize(0);
        final VirtuosoTripleTransaction tripleTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement1 = createFakeStatement();
        final Statement testStatement2 = createFakeStatement();
        tripleTransaction.add(testStatement1);
        tripleTransaction.add(testStatement2);
        checkStatementExists(tripleTransaction, testStatement1, true);
        checkStatementExists(tripleTransaction, testStatement1, true);
        tripleTransaction.end(false);

        final TripleTransaction testTransaction = this.store.begin(false);
        checkStatementExists(testTransaction, testStatement1, false);
        checkStatementExists(testTransaction, testStatement2, false);
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#remove(Statement)}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testRemove() throws IOException {
        final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement = createFakeStatement();
        transaction.add(testStatement);
        checkStatementExists(transaction, testStatement, true);
        transaction.remove(testStatement);
        checkStatementExists(transaction, testStatement, false);
        transaction.end(true);

        // Post condition: transaction persistence.
        final TripleTransaction checkTransaction = this.store.begin(true);
        checkStatementExists(checkTransaction, testStatement, false);
        checkTransaction.end(false);
    }

    /**
     * Test the remove - rollback transaction work.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testRemoveRollback() throws IOException {
        final VirtuosoTripleTransaction addTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final Statement testStatement = createFakeStatement();
        addTransaction.add(testStatement);
        addTransaction.end(true);

        final VirtuosoTripleTransaction deleteTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        checkStatementExists(deleteTransaction, testStatement, true);
        deleteTransaction.remove(testStatement);
        checkStatementExists(deleteTransaction, testStatement, false);
        deleteTransaction.end(false);

        final TripleTransaction checkTransaction = this.store.begin(true);
        checkStatementExists(checkTransaction, testStatement, true);
        checkTransaction.end(false);
    }

    /**
     * Test the removeBulk - rollback transaction work.
     *
     * @throws IOException
     *             on failure
     */
    @Ignore
    @Test
    public void testRemoveBulkRollback() throws IOException {
        final Statement[] testStatements = new Statement[] { createFakeStatement(),
                createFakeStatement(), createFakeStatement(), createFakeStatement() };
        final VirtuosoTripleTransaction addTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        addTransaction.addBulk(Arrays.asList(testStatements), true);
        addTransaction.end(true);

        final VirtuosoTripleTransaction deleteTransaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        for (final Statement statement : testStatements) {
            checkStatementExists(deleteTransaction, statement, true);
        }
        deleteTransaction.removeBulk(Arrays.asList(testStatements), true);
        for (final Statement statement : testStatements) {
            checkStatementExists(deleteTransaction, statement, false);
        }
        deleteTransaction.end(false);

        final TripleTransaction checkTransaction = this.store.begin(true);
        for (final Statement statement : testStatements) {
            checkStatementExists(checkTransaction, statement, true);
        }
        checkTransaction.end(false);
    }

    /**
     * Tests bulk removal.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testRemoveBulk() throws IOException {
        final String uuid = UUID.randomUUID().toString();
        final Iterable<Statement> added = createFakeStatements(uuid, BULK_STATEMENT_COUNT);
        final VirtuosoTripleTransaction transaction = (VirtuosoTripleTransaction) this.store
                .begin(false);
        final int initialTriples = countStatementsInStore(transaction);
        transaction.addBulk(added, false);
        final int triplesAfterAdd = countStatementsInStore(transaction);
        assertEquals(BULK_STATEMENT_COUNT, triplesAfterAdd - initialTriples);

        final Iterable<Statement> removed = createFakeStatements(uuid, BULK_STATEMENT_COUNT);
        final long beginTime = System.currentTimeMillis();
        transaction.removeBulk(removed, false);
        final int triplesAfterRemove = countStatementsInStore(transaction);
        transaction.end(true);
        assertEquals(initialTriples, triplesAfterRemove);
        final long endTime = System.currentTimeMillis();
        final long elapsed = endTime - beginTime;
        LOG.debug("Removed statements: {}, Elapsed time: {} ms, statements/ms: {}\n",
                BULK_STATEMENT_COUNT, elapsed, BULK_STATEMENT_COUNT / (float) elapsed);
    }

    /**
     * Test for {@link TripleTransaction#remove(Stream)}.
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testRemoveByCursor() throws IOException {
        final int triplesCount = CURSOR_STATEMENT_COUNT;
        final String uuid = UUID.randomUUID().toString();
        final Iterable<Statement> added = createFakeStatements(uuid, triplesCount);
        final TripleTransaction addTransaction = this.store.begin(false);
        final int triplesBefore = countStatementsInStore(addTransaction);
        addTransaction.add(Stream.create(added));
        addTransaction.end(true);
        final int triplesAfter = countStatementsInStore(null);
        assertEquals(triplesCount, triplesAfter - triplesBefore);

        final Iterable<Statement> removed = createFakeStatements(uuid, triplesCount);
        final TripleTransaction removeTransaction = this.store.begin(false);
        removeTransaction.remove(Stream.create(removed));
        removeTransaction.end(true);
        final int afterRemove = countStatementsInStore(null);
        assertEquals(triplesBefore, afterRemove);
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#query(SelectQuery, BindingSet)} .
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testQuery() throws IOException {
        final TripleTransaction transaction = this.store.begin(true);
        final CloseableIteration<BindingSet, QueryEvaluationException> cursor = transaction.query(
                SelectQuery.from("SELECT * WHERE {?s ?p ?o}"), null, null);
        int statementsCount = 0;
        try {
            while (cursor.hasNext()) {
                final BindingSet current = cursor.next();
                assertTrue(current.hasBinding("s"));
                assertTrue(current.hasBinding("p"));
                assertTrue(current.hasBinding("o"));
                assertNotNull(current.getBinding("s").getValue());
                assertNotNull(current.getBinding("p").getValue());
                assertNotNull(current.getBinding("o").getValue());
                statementsCount++;
            }
        } catch (final QueryEvaluationException ex) {
            throw new IOException(ex);
        }
        assertEquals(NUM_OF_DEFAULT_STATEMENTS, statementsCount);
    }

    /**
     * Test for {@link VirtuosoTripleTransaction#infer(Handler)} .
     *
     * @throws IOException
     *             on failure
     */
    @Test
    public void testInfer() throws IOException {
        final int pre = countStatementsInStore(null);
        final TripleTransaction transaction = this.store.begin(false);
        final boolean[] doneReached = new boolean[] { false };
        transaction.infer(new Handler<Statement>() {

            @Override
            public void handle(@Nullable final Statement element) {
                if (element == null) {
                    doneReached[0] = true;
                }
            }
        });
        final int post = countStatementsInStore(null);
        assertTrue(doneReached[0]);
        assertEquals(0, post - pre);
    }

    /**
     * Tests for {@link #createFakeStatements(int, int)} method.
     */
    @Test
    public void testCreateFakeStatements() {
        final int numContexts = 10;
        final int numStatements = 100;
        final Set<String> contexts = new HashSet<String>();
        final Set<String> statements = new HashSet<String>();
        int counter = 0;
        for (final Statement statement : createFakeStatements(numStatements, numContexts)) {
            contexts.add(statement.getContext().stringValue());
            assertTrue(statements.add(statement.getSubject().stringValue()
                    + statement.getPredicate().stringValue() + statement.getObject().stringValue()
                    + statement.getContext().stringValue()));
            counter++;
        }
        assertEquals(numStatements, counter);
        assertEquals(numContexts, contexts.size());
        assertEquals(numStatements, statements.size());
    }

    private Iterable<Statement> createFakeStatements(final String uuid, final int statements,
            final int contexts) {
        return new Iterable<Statement>() {

            @Override
            public Iterator<Statement> iterator() {
                return new StatementsGenerator(uuid, statements, contexts);
            }
        };
    }

    private Iterable<Statement> createFakeStatements(final String uuid, final int statements) {
        return createFakeStatements(uuid, statements, 1);
    }

    private Iterable<Statement> createFakeStatements(final int statements, final int contexts) {
        return createFakeStatements(UUID.randomUUID().toString(), statements, contexts);
    }

    private Iterable<Statement> createFakeStatements(final int statements) {
        return createFakeStatements(UUID.randomUUID().toString(), statements, 1);
    }

    private ContextStatementImpl createFakeStatement(final String pack, final String uuid,
            final int statementID, final int contextID) {
        return new ContextStatementImpl(new URIImpl(String.format("http://%s/sub#%s/%d", pack,
                uuid, statementID)), new URIImpl(String.format("http://%s/pre#%s/%d", pack, uuid,
                statementID)), new URIImpl(String.format("http://%s/obj#%s/%d", pack, uuid,
                statementID)), new URIImpl(String.format("http://%s/ctx#%s/%d", pack, uuid,
                contextID)));
    }

    private ContextStatementImpl createFakeStatement() {
        final String packageName = this.getClass().getPackage().getName();
        final UUID uuid = UUID.randomUUID();
        return createFakeStatement(packageName, uuid.toString(), 0, 0);
    }

    private int countStatementsInStore(@Nullable final TripleTransaction transaction)
            throws IOException {
        final TripleTransaction tx = transaction != null ? transaction : this.store.begin(true);
        try {
            final CloseableIteration<BindingSet, QueryEvaluationException> cursor = tx.query(
                    SelectQuery.from("SELECT * WHERE {?s ?p ?o}"), null, null);
            int statementsCount = 0;
            while (cursor.hasNext()) {
                cursor.next();
                statementsCount++;
            }
            return statementsCount;
        } catch (final QueryEvaluationException ex) {
            throw new IOException(ex);
        } finally {
            if (transaction == null) {
                tx.end(true);
            }
        }
    }

    private void checkStatementExists(final TripleTransaction tripleTransaction,
            final Statement target, final boolean exists) throws IOException {
        try {
            final CloseableIteration<BindingSet, QueryEvaluationException> cursor;
            cursor = tripleTransaction.query(SelectQuery.from(String.format(
                    "SELECT * WHERE {<%s> <%s> <%s>}", target.getSubject().stringValue(), target
                            .getPredicate().stringValue(), target.getObject().stringValue())),
                    null, null);
            assertEquals(exists, cursor.hasNext());
            cursor.close();
        } catch (final QueryEvaluationException ex) {
            throw new IOException(ex);
        }
    }

    private class StatementsGenerator implements Iterator<Statement> {

        final String packageName = this.getClass().getPackage().getName();
        final String uuid;
        final int statements;
        final int contextSwitch;

        private int emitted = 0;

        StatementsGenerator(final String uuid, final int statements, final int contexts) {
            if (contexts <= 0) {
                throw new IllegalArgumentException();
            }
            if (contexts > statements) {
                throw new IllegalArgumentException();
            }

            this.uuid = uuid == null ? UUID.randomUUID().toString() : uuid;
            this.statements = statements;
            this.contextSwitch = statements / contexts;
        }

        @Override
        public boolean hasNext() {
            return this.emitted < this.statements;
        }

        @Override
        public Statement next() {
            if (this.emitted >= this.statements) {
                throw new NoSuchElementException();
            }
            final Statement s = createFakeStatement(this.packageName, this.uuid, this.emitted,
                    this.emitted / this.contextSwitch);
            this.emitted++;
            return s;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
