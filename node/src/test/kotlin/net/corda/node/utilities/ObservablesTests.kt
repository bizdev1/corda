package net.corda.node.utilities

import com.google.common.util.concurrent.SettableFuture
import net.corda.core.ThreadBox
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.VaultService
import net.corda.node.services.config.NodeConfiguration
import net.corda.testing.node.MockNetwork
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import rx.Observable
import rx.subjects.PublishSubject
import java.security.KeyPair
import kotlin.test.assertNotNull


class ObservablesTests {

    @Test
            // Note cannot use quoted method name as compiler fails to escape name when generating class names.
    fun exportedFromWithinNodeIsRawAndExportedFromOutsideNodeIsDeferred() {
        val rawObservable = PublishSubject.create<Unit>()
        var unexportedObservable: Observable<Unit>? = null

        // Create a mock node, and then register an observer a) during start up and b) after startup.
        val network = MockNetwork(defaultFactory = object : MockNetwork.Factory {
            override fun create(config: NodeConfiguration, network: MockNetwork, networkMapAddr: SingleMessageRecipient?, advertisedServices: Set<ServiceInfo>, id: Int, keyPair: KeyPair?): MockNetwork.MockNode {
                return object : MockNetwork.MockNode(config, network, networkMapAddr, advertisedServices, id, keyPair) {
                    override fun makeVaultService(): VaultService {
                        // Export from within node.
                        unexportedObservable = rawObservable.export(services)
                        return super.makeVaultService()
                    }
                }
            }
        })
        val node = network.createSomeNodes(numPartyNodes = 1).partyNodes[0]
        // Export from outside node.
        val exportedObservable: Observable<Unit> = rawObservable.export(node.services)

        // Setup listeners and then send observation.
        val eventSeqNo = ThreadBox(object {
            var value = 0
        })
        val exportedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()
        val unexportedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()
        val mainThread = Thread.currentThread()

        assertNotNull(unexportedObservable, "unexportedObservable is null")
        exportedObservable.subscribe { exportedEventSeqNo.set(eventSeqNo.locked { value++ } to (Thread.currentThread() == mainThread)) }
        unexportedObservable!!.subscribe { unexportedEventSeqNo.set(eventSeqNo.locked { value++ } to (Thread.currentThread() == mainThread)) }

        assertThat(rawObservable).isNotEqualTo(exportedObservable)
        assertThat(rawObservable).isEqualTo(unexportedObservable)

        // Use the re-entrant locking to ensure same thread subscriber runs first, as other thread locked out.
        eventSeqNo.locked { rawObservable.onNext(Unit) }

        // Unexported should fire first and be on main thread as it is synchronous.
        assertThat(unexportedEventSeqNo.get()).isEqualTo(0 to true)
        // Exported should fire second and not be on main thread as asynchronous on server thread.
        assertThat(exportedEventSeqNo.get()).isEqualTo(1 to false)

        network.stopNodes()
    }
}