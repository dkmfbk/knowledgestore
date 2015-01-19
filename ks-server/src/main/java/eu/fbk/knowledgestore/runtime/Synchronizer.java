package eu.fbk.knowledgestore.runtime;

import java.util.concurrent.Semaphore;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public final class Synchronizer {

    public static final int WX = -1;

    public static final int CX = -2;

    private final int maxConcurrentTx;

    private final int maxWriteTx; // can assume special values 0, WX or CX

    private final Semaphore mainSemaphore;

    @Nullable
    private final Semaphore writeSemaphore;

    @Nullable
    private final Semaphore commitSemaphore;

    private Synchronizer(final int maxConcurrentTx, final int maxWriteTx) {

        Preconditions.checkArgument(maxConcurrentTx > 0);
        Preconditions.checkArgument(maxWriteTx >= 0 && maxWriteTx <= maxConcurrentTx
                || maxWriteTx == WX || maxWriteTx == CX);

        this.maxConcurrentTx = maxConcurrentTx;
        this.maxWriteTx = maxWriteTx;
        this.mainSemaphore = new Semaphore(maxConcurrentTx, true);
        this.writeSemaphore = maxWriteTx != 0 ? new Semaphore(Math.max(maxWriteTx, 1), true)
                : null;
        this.commitSemaphore = maxWriteTx == CX ? new Semaphore(1, true) : null;
    }

    public static Synchronizer create(final String spec) {

        final int index = spec.indexOf(':');
        final String first = (index <= 0 ? spec : spec.substring(0, index)).trim();
        final String second = index <= 0 ? null : spec.substring(index + 1).trim().toUpperCase();

        try {
            final int maxConcurrentTx = Integer.parseInt(first);
            final int maxWriteTx = second == null ? 0 : second.equals("WX") ? WX : //
                    second.equals("CX") ? CX : Integer.parseInt(second);
            return new Synchronizer(maxConcurrentTx, maxWriteTx);
        } catch (final Throwable ex) {
            throw new IllegalArgumentException(
                    "Illegal synchronizer specification '" + spec + "'", ex);
        }
    }

    public static Synchronizer create(final int maxConcurrentTx, final int maxWriteTx) {
        return new Synchronizer(maxConcurrentTx, maxWriteTx);
    }

    public void beginExclusive() {
        try {
            this.mainSemaphore.acquire(this.maxConcurrentTx);
        } catch (final Throwable ex) {
            Throwables.propagate(ex);
        }
    }

    public void endExclusive() {
        this.mainSemaphore.release(this.maxConcurrentTx);
    }

    public void beginTransaction(final boolean readOnly) {
        boolean writeAcquired = false;
        try {
            if (readOnly) {
                this.mainSemaphore.acquire();
            } else if (this.writeSemaphore == null) {
                throw new IllegalStateException("Write transactions have been disabled");
            } else {
                this.writeSemaphore.acquire();
                writeAcquired = true;
                this.mainSemaphore.acquire(this.maxWriteTx == WX ? this.maxConcurrentTx : 1);
            }
        } catch (final Throwable ex) {
            if (writeAcquired) {
                this.writeSemaphore.release();
            }
            Throwables.propagate(ex);
        }
    }

    public void endTransaction(final boolean readOnly) {
        if (readOnly) {
            this.mainSemaphore.release(1);
        } else {
            this.mainSemaphore.release(this.maxWriteTx == WX ? this.maxConcurrentTx : 1);
            this.writeSemaphore.release();
        }
    }

    public void beginCommit() {
        if (this.maxWriteTx == CX) {
            boolean commitAcquired = false;
            try {
                this.commitSemaphore.acquire();
                commitAcquired = true;
                this.mainSemaphore.acquire(this.maxConcurrentTx - 1);
            } catch (final Throwable ex) {
                if (commitAcquired) {
                    this.commitSemaphore.release();
                }
                Throwables.propagate(ex);
            }
        }
    }

    public void endCommit() {
        if (this.maxWriteTx == CX) {
            this.mainSemaphore.release(this.maxConcurrentTx - 1);
            this.commitSemaphore.release();
        }
    }

    @Override
    public String toString() {
        return this.maxConcurrentTx + ":"
                + (this.maxWriteTx == WX ? "WX" : this.maxWriteTx == CX ? "CX" : this.maxWriteTx);
    }

}
