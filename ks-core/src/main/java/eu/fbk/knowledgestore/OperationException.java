package eu.fbk.knowledgestore;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

// TODO: make OperationException unchecked?

/**
 * Signals the failure of a KnowledgeStore {@code Operation} invocation.
 * <p>
 * This exception is thrown every time an invocation of a KnowledgeStore {@link Operation} fails.
 * The outcome attribute (see {@link #getOutcome()}) provides the error / unknown operation
 * outcome at the root of this exception. The {@code Throwable}s causing this exception (zero or
 * more) are also stored: method {@link #getCauses()} returns a list with all the causes, while
 * standard method {@link #getCause()} returns only the last cause of the list (thus, only this
 * cause will be included in the stacktrace printed by {@link #printStackTrace()}).
 * </p>
 */
public class OperationException extends Exception {

    private static final long serialVersionUID = 1L;

    private final Outcome outcome;

    private final List<Throwable> causes;

    /**
     * Creates a new instance with the outcome and causes vararg array specified.
     * 
     * @param outcome
     *            the error/unknown outcome for this exception
     * @param causes
     *            a vararg array with the causes of this exception, possibly empty
     */
    public OperationException(final Outcome outcome, final Throwable... causes) {
        this(outcome, ImmutableList.copyOf(causes));
    }

    /**
     * Creates a new instance with the outcome and causes {@code Iterable} specified.
     * 
     * @param outcome
     *            the error/unknown outcome for this exception
     * @param causes
     *            an iterable with the causes of this exception, not null, possibly empty
     */
    public OperationException(final Outcome outcome, final Iterable<? extends Throwable> causes) {
        super(messageFor(Preconditions.checkNotNull(outcome), causes),
                Iterables.isEmpty(causes) ? null : Iterables.getLast(causes));
        this.outcome = outcome;
        this.causes = ImmutableList.copyOf(causes);
    }

    /**
     * Returns the error/unknown outcome associated to this exception.
     * 
     * @return the outcome
     */
    public Outcome getOutcome() {
        return this.outcome;
    }

    /**
     * Returns all the causes of this exception. Causes are reported whenever available; in
     * particular, {@code OperationException} on single objects are reported in case of
     * {@code create}, {@code merge}, {@code update}, {@code delete} bulk operations.
     * 
     * @return the causes associated to this exception
     */
    public List<Throwable> getCauses() {
        return this.causes;
    }

    private static String messageFor(final Outcome outcome,
            final Iterable<? extends Throwable> causes) {
        final StringBuilder builder = new StringBuilder();
        builder.append(outcome);
        int index = 0;
        for (final Throwable cause : causes) {
            builder.append("\n(").append(++index).append(") ")
                    .append(cause.getClass().getSimpleName()).append(" ")
                    .append(cause.getMessage());
        }
        return builder.toString();
    }

}
