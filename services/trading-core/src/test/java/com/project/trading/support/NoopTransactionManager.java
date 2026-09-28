package com.project.trading.support;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/** A transaction manager for in-memory tests: transactions have no effect. */
public class NoopTransactionManager extends AbstractPlatformTransactionManager {

    private static final long serialVersionUID = 1L;

    @Override
    protected Object doGetTransaction() {
        return new Object();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        // Nothing to begin.
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        // Nothing to commit.
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        // Nothing to roll back.
    }
}
