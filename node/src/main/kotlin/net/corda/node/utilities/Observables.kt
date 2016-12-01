package net.corda.node.utilities

import net.corda.core.node.ServiceHub
import net.corda.node.internal.AbstractNode
import rx.Observable

fun <T : Any> Observable<T>.export(services: ServiceHub): Observable<T> {
    val stackTrace = Thread.currentThread().stackTrace
    for (frame in stackTrace) {
        if (frame.className == AbstractNode::class.qualifiedName) {
            return this
        }
    }
    return this.observeOn(services.externalObservationScheduler)
}