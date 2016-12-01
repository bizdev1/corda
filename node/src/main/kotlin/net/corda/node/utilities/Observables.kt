package net.corda.node.utilities

import net.corda.core.node.ServiceHub
import net.corda.node.internal.AbstractNode
import rx.Observable

/**
 * Select an appropriate [Scheduler] to deliver observations on, dependent on whether the subscriber is inside or
 * outside the node, and return an [Observable] that observes on that [Scheduler].
 *
 * Those outside the node should be notified after any [Flow] has checkpointed and committed the associated database
 * transaction so that changes are externally visible.  Otherwise the state associated with the observation might not
 * yet have changed (been committed) to reflect it. If within the node, then it's okay to execute immediately on the
 * [Fiber] and/or within the same database transaction.
 */
fun <T : Any> Observable<T>.export(services: ServiceHub): Observable<T> {
    val stackTrace = Thread.currentThread().stackTrace
    for (frame in stackTrace) {
        if (frame.className == AbstractNode::class.qualifiedName) {
            return this
        }
    }
    return this.observeOn(services.externalObservationScheduler)
}