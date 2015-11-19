package core

import contracts.Cash
import core.testutils.*
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class TransactionGroupTests {
    val A_THOUSAND_POUNDS = Cash.State(InstitutionReference(MINI_CORP, OpaqueBytes.of(1, 2, 3)), 1000.POUNDS, MINI_CORP_KEY)

    @Test
    fun success() {
        transactionGroup {
            roots {
                transaction(A_THOUSAND_POUNDS label "£1000")
            }

            transaction {
                input("£1000")
                output("alice's £1000") { A_THOUSAND_POUNDS `owned by` ALICE }
                arg(MINI_CORP_KEY) { Cash.Commands.Move }
            }

            transaction {
                input("alice's £1000")
                arg(ALICE) { Cash.Commands.Move }
                arg(MINI_CORP_KEY) { Cash.Commands.Exit(1000.POUNDS) }
            }

            verify()
        }
    }

    @Test
    fun conflict() {
        transactionGroup {
            val t = transaction {
                output("cash") { A_THOUSAND_POUNDS }
                arg(MINI_CORP_KEY) { Cash.Commands.Issue() }
            }

            val conflict1 = transaction {
                input("cash")
                val HALF = A_THOUSAND_POUNDS.copy(amount = 500.POUNDS) `owned by` BOB
                output { HALF }
                output { HALF }
                arg(MINI_CORP_KEY) { Cash.Commands.Move }
            }

            verify()

            // Alice tries to double spend back to herself.
            val conflict2 = transaction {
                input("cash")
                val HALF = A_THOUSAND_POUNDS.copy(amount = 500.POUNDS) `owned by` ALICE
                output { HALF }
                output { HALF }
                arg(MINI_CORP_KEY) { Cash.Commands.Move }
            }

            assertNotEquals(conflict1, conflict2)

            val e = assertFailsWith(TransactionConflictException::class) {
                verify()
            }
            assertEquals(ContractStateRef(t.hash, 0), e.conflictRef)
            assertEquals(setOf(conflict1, conflict2), setOf(e.tx1, e.tx2))
        }
    }

    @Test
    fun disconnected() {
        // Check that if we have a transaction in the group that doesn't connect to anything else, it's rejected.
        val tg = transactionGroup {
            transaction {
                output("cash") { A_THOUSAND_POUNDS }
                arg(MINI_CORP_KEY) { Cash.Commands.Issue() }
            }

            transaction {
                input("cash")
                output { A_THOUSAND_POUNDS `owned by` BOB }
            }
        }

        // We have to do this manually without the DSL because transactionGroup { } won't let us create a tx that
        // points nowhere.
        val ref = ContractStateRef(SecureHash.randomSHA256(), 0)
        tg.txns.add(LedgerTransaction(
                listOf(ref), listOf(A_THOUSAND_POUNDS), listOf(AuthenticatedObject(listOf(BOB), emptyList(), Cash.Commands.Move)), TEST_TX_TIME, SecureHash.randomSHA256())
        )

        val e = assertFailsWith(TransactionResolutionException::class) {
            tg.verify()
        }
        assertEquals(e.hash, ref.txhash)
    }
}