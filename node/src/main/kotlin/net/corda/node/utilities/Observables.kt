package net.corda.node.utilities

import rx.Observable

/**
 * Delay observations until after the current database transaction has been committed.
 *
 * Those outside the node should be notified after any [Flow] has checkpointed and committed the associated database
 * transaction so that changes are externally visible.  Otherwise the state associated with the observation might not
 * yet have changed (been committed) to reflect it. If within the node, then it's okay to execute immediately on the
 * [Fiber] and/or within the same database transaction.
 */
fun <T : Any> Observable<T>.afterCommit(): Observable<T> {
    val databaseTxBoundaries: Observable<StrandLocalTransactionManager.Boundary> = StrandLocalTransactionManager.transactionBoundaries
    return this.buffer(databaseTxBoundaries).concatMap { Observable.from(it) }
}