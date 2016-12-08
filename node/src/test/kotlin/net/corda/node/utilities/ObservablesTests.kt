package net.corda.node.utilities

import com.google.common.util.concurrent.SettableFuture
import net.corda.testing.node.makeTestDataSourceProperties
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.junit.Test
import rx.Observable
import rx.subjects.PublishSubject

class ObservablesTests {

    @Test
    fun `afterCommit delays until outside transaction again`() {
        val (toBeClosed, database) = configureDatabase(makeTestDataSourceProperties())

        val subject = PublishSubject.create<Unit>()
        val undelayedObservable: Observable<Unit> = subject
        val delayedObservable: Observable<Unit> = subject.afterDatabaseCommit()

        val delayedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()
        val undelayedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()

        // Setup listeners and then send observation.
        var value = 0
        delayedObservable.subscribe { delayedEventSeqNo.set(value++ to isInDatabaseTransaction()) }
        undelayedObservable!!.subscribe { undelayedEventSeqNo.set(value++ to isInDatabaseTransaction()) }

        assertThat(subject).isNotEqualTo(delayedObservable)
        assertThat(subject).isEqualTo(undelayedObservable)

        databaseTransaction(database) {
            subject.onNext(Unit)
        }

        // Undelayed should fire first and be inside the transaction.
        assertThat(undelayedEventSeqNo.get()).isEqualTo(0 to true)
        // delayed should fire second and be outside the transaction.
        assertThat(delayedEventSeqNo.get()).isEqualTo(1 to false)

        toBeClosed.close()
    }

    private fun isInDatabaseTransaction(): Boolean = (TransactionManager.currentOrNull() != null)

    @Test
    fun `bufferUntilDatabaseCommit delays until transaction closed`() {
        val (toBeClosed, database) = configureDatabase(makeTestDataSourceProperties())

        val subject = PublishSubject.create<Int>()
        val observable: Observable<Int> = subject

        val firstEvent = SettableFuture.create<Pair<Int, Boolean>>()
        val secondEvent = SettableFuture.create<Pair<Int, Boolean>>()

        observable.first().subscribe { firstEvent.set(it to isInDatabaseTransaction()) }
        observable.skip(1).first().subscribe { secondEvent.set(it to isInDatabaseTransaction()) }

        databaseTransaction(database) {
            val delayedSubject = subject.bufferUntilDatabaseCommit()
            assertThat(subject).isNotEqualTo(delayedSubject)
            delayedSubject.onNext(0)
            subject.onNext(1)
            assertThat(firstEvent.isDone).isTrue()
            assertThat(secondEvent.isDone).isFalse()
        }
        assertThat(secondEvent.isDone).isTrue()

        assertThat(firstEvent.get()).isEqualTo(1 to true)
        assertThat(secondEvent.get()).isEqualTo(0 to false)

        toBeClosed.close()
    }

    @Test
    fun `bufferUntilDatabaseCommit delays until transaction closed repeatable`() {
        val (toBeClosed, database) = configureDatabase(makeTestDataSourceProperties())

        val subject = PublishSubject.create<Int>()
        val observable: Observable<Int> = subject

        val firstEvent = SettableFuture.create<Pair<Int, Boolean>>()
        val secondEvent = SettableFuture.create<Pair<Int, Boolean>>()

        observable.first().subscribe { firstEvent.set(it to isInDatabaseTransaction()) }
        observable.skip(1).first().subscribe { secondEvent.set(it to isInDatabaseTransaction()) }

        databaseTransaction(database) {
            val delayedSubject = subject.bufferUntilDatabaseCommit()
            assertThat(subject).isNotEqualTo(delayedSubject)
            delayedSubject.onNext(0)
            assertThat(firstEvent.isDone).isFalse()
            assertThat(secondEvent.isDone).isFalse()
        }
        assertThat(firstEvent.isDone).isTrue()
        assertThat(secondEvent.isDone).isFalse()

        databaseTransaction(database) {
            val delayedSubject = subject.bufferUntilDatabaseCommit()
            assertThat(subject).isNotEqualTo(delayedSubject)
            delayedSubject.onNext(1)
            assertThat(firstEvent.isDone).isTrue()
            assertThat(secondEvent.isDone).isFalse()
        }
        assertThat(secondEvent.isDone).isTrue()

        assertThat(firstEvent.get()).isEqualTo(0 to false)
        assertThat(secondEvent.get()).isEqualTo(1 to false)

        toBeClosed.close()
    }
}