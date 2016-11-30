package net.corda.node.utilities

import net.corda.core.node.ServiceHub
import net.corda.node.internal.AbstractNode
import rx.Observable

fun <T : Any> Observable<T>.export(services: ServiceHub): Observable<T> {
    val stackTrace = Thread.currentThread().stackTrace
    for (frame in stackTrace) {
        //println("class name = ${frame.className} ${AbstractNode::class.qualifiedName}")
        if (frame.className == AbstractNode::class.qualifiedName) {
            //println("*** Result = internal")
            return this
        }
    }
    //println("*** Result = external")
    //println("*** Externalise ${stackTrace.joinToString("\n")}")
    return this.observeOn(services.externallyObservableScheduler)
}