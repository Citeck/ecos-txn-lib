package ru.citeck.ecos.txn.lib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;
import java.lang.reflect.Field;
import ru.citeck.ecos.txn.lib.app.TestTxnApp;
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo;
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps;
import ru.citeck.ecos.txn.lib.transaction.Transaction;
import ru.citeck.ecos.webapp.api.func.UncheckedRunnable;
import ru.citeck.ecos.webapp.api.func.UncheckedSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TxnContextJavaContractTest {

    private TestTxnApp app;

    @BeforeEach
    void setUp() {
        app = new TestTxnApp("java-contract", new EcosTxnProps());
        app.init(NoopTwoPhaseCommitRepo.INSTANCE);
        TxnContext.INSTANCE.setManager(app.getTxnManager());
    }

    @AfterEach
    void tearDown() {
        app.getTxnManager().shutdown();
    }

    @Test
    void supplierOverloadsReturnValuesAndRunnableOverloadsJoinOrSuspend() {
        Object result = new Object();
        assertThat(TxnContext.doInTxnJ((UncheckedSupplier<Object>) () -> {
            Transaction outer = TxnContext.getTxn();
            assertThat(outer.isReadOnly()).isFalse();
            TxnContext.doInTxnJ((UncheckedRunnable) () -> assertThat(TxnContext.getTxn()).isSameAs(outer));
            TxnContext.doInTxnJ(true, (UncheckedRunnable) () -> {
                assertThat(TxnContext.getTxn()).isSameAs(outer);
                assertThat(TxnContext.isReadOnly()).isTrue();
            });
            assertThat(TxnContext.isReadOnly()).isFalse();
            TxnContext.doInNewTxnJ((UncheckedRunnable) () -> {
                assertThat(TxnContext.getTxn()).isNotSameAs(outer);
                assertThat(TxnContext.isReadOnly()).isFalse();
            });
            TxnContext.doInNewTxnJ(true, (UncheckedRunnable) () -> {
                assertThat(TxnContext.getTxn()).isNotSameAs(outer);
                assertThat(TxnContext.isReadOnly()).isTrue();
            });
            assertThat(TxnContext.doInNewTxnJ((UncheckedSupplier<Object>) () -> result)).isSameAs(result);
            assertThat(TxnContext.doInNewTxnJ(true, (UncheckedSupplier<Transaction>) () -> {
                assertThat(TxnContext.isReadOnly()).isTrue();
                return TxnContext.getTxn();
            })).isNotSameAs(outer);
            assertThat(TxnContext.doInTxnJ(true, (UncheckedSupplier<Transaction>) TxnContext::getTxn)).isSameAs(outer);
            assertThat(TxnContext.getTxn()).isSameAs(outer);
            return result;
        })).isSameAs(result);
        assertThat(TxnContext.getTxnOrNull()).isNull();
    }

    @Test
    void checkedExceptionsPreserveIdentityAndRestoreParentReadOnlyAndMdc() throws Exception {
        Field adapterField = MDC.class.getDeclaredField("mdcAdapter");
        adapterField.setAccessible(true);
        MDCAdapter originalAdapter = MDC.getMDCAdapter();
        adapterField.set(null, new BasicMDCAdapter());
        IOException failure = new IOException("checked failure");
        MDC.put("ecosTxnId", "caller");
        try {
            TxnContext.doInTxnJ((UncheckedRunnable) () -> {
                Transaction parent = TxnContext.getTxn();
                String parentMdc = MDC.get("ecosTxnId");
                assertThatThrownBy(() -> TxnContext.doInTxnJ(true, (UncheckedSupplier<Object>) () -> {
                    throw failure;
                })).isSameAs(failure);
                assertThat(TxnContext.getTxn()).isSameAs(parent);
                assertThat(TxnContext.isReadOnly()).isFalse();
                assertThatThrownBy(() -> TxnContext.doInNewTxnJ(true, (UncheckedRunnable) () -> {
                    throw failure;
                })).isSameAs(failure);
                assertThat(TxnContext.getTxn()).isSameAs(parent);
                assertThat(MDC.get("ecosTxnId")).isEqualTo(parentMdc);
            });
            assertThatThrownBy(() -> TxnContext.doInTxnJ((UncheckedSupplier<Object>) () -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(TxnContext.getTxnOrNull()).isNull();
            assertThat(MDC.get("ecosTxnId")).isEqualTo("caller");
        } finally {
            adapterField.set(null, originalAdapter);
        }
    }

    @Test
    void consumersBatchInInsertionOrderAndLifecycleWrappersRespectCommit() throws Exception {
        List<List<String>> before = new ArrayList<>();
        List<List<String>> after = new CopyOnWriteArrayList<>();
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch completed = new CountDownLatch(3);
        TxnContext.doInTxnJ((UncheckedRunnable) () -> {
            for (String item : List.of("b", "a", "b")) {
                TxnContext.processSetBeforeCommitJ("before-set", item, items -> before.add(new ArrayList<>(items)));
                TxnContext.processListBeforeCommitJ("before-list", item, items -> before.add(new ArrayList<>(items)));
                TxnContext.processSetAfterCommitJ("after-set", item, items -> {
                    after.add(new ArrayList<>(items));
                    completed.countDown();
                });
                TxnContext.processListAfterCommitJ("after-list", item, items -> {
                    after.add(new ArrayList<>(items));
                    completed.countDown();
                });
            }
            TxnContext.doBeforeCommitJ(2f, () -> events.add("second"));
            TxnContext.doBeforeCommitJ(-1f, () -> events.add("first"));
            TxnContext.doAfterCommitJ(0f, false, () -> {
                events.add("commit");
                completed.countDown();
            });
            TxnContext.doAfterRollbackJ(0f, false, () -> events.add("rollback"));
            assertThat(before).isEmpty();
            assertThat(after).isEmpty();
        });
        assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(before).containsExactly(List.of("b", "a"), List.of("b", "a", "b"));
        assertThat(after).containsExactlyInAnyOrder(List.of("b", "a"), List.of("b", "a", "b"));
        assertThat(events).containsExactly("first", "second", "commit");
    }

    @Test
    void rollbackDropsCommitConsumersAndRunsJavaRollbackAction() {
        List<String> events = new CopyOnWriteArrayList<>();
        IOException failure = new IOException("abort");
        assertThatThrownBy(() -> TxnContext.doInTxnJ((UncheckedRunnable) () -> {
            TxnContext.processListBeforeCommitJ("before", "item", items -> events.add("before"));
            TxnContext.processSetAfterCommitJ("after", "item", items -> events.add("after"));
            TxnContext.doAfterRollbackJ(0f, false, () -> events.add("rollback"));
            throw failure;
        })).isSameAs(failure);
        assertThat(events).containsExactly("rollback");
    }
    @Test
    void absentManagerRunsJavaActionsImmediatelyWithoutLeakingGlobalConfiguration() throws Exception {
        Field field = TxnContext.class.getDeclaredField("managerExists");
        field.setAccessible(true);
        boolean previous = field.getBoolean(TxnContext.INSTANCE);
        List<String> events = new ArrayList<>();
        IOException failure = new IOException("no-manager checked failure");
        try {
            field.setBoolean(TxnContext.INSTANCE, false);
            TxnContext.doInTxnJ((UncheckedRunnable) () -> events.add("required"));
            assertThat(TxnContext.doInNewTxnJ(true, (UncheckedSupplier<String>) () -> {
                assertThat(TxnContext.getTxnOrNull()).isNull();
                assertThat(TxnContext.isReadOnly()).isFalse();
                return "result";
            })).isEqualTo("result");
            TxnContext.doBeforeCommitJ(0f, () -> events.add("before"));
            TxnContext.doAfterCommitJ(0f, false, () -> events.add("after"));
            TxnContext.doAfterRollbackJ(0f, false, () -> events.add("rollback"));
            TxnContext.processSetBeforeCommitJ("set", "a", values -> events.add("set:" + values));
            TxnContext.processListAfterCommitJ("list", "a", values -> events.add("list:" + values));
            assertThatThrownBy(() -> TxnContext.doInTxnJ((UncheckedRunnable) () -> {
                throw failure;
            })).isSameAs(failure);
            assertThatThrownBy(TxnContext::getTxn).hasMessageContaining("doesn't exists");
            assertThat(events).containsExactly("required", "before", "after", "set:[a]", "list:[a]");
        } finally {
            field.setBoolean(TxnContext.INSTANCE, previous);
        }
        assertThat(TxnContext.INSTANCE.getManager()).isSameAs(app.getTxnManager());
        TxnContext.doInTxnJ((UncheckedRunnable) () -> assertThat(TxnContext.getTxn()).isNotNull());
    }

}
