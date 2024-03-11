package ru.citeck.ecos.txn.lib.manager.api.server

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.utils.ReflectUtils
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.manager.api.server.action.*
import ru.citeck.ecos.txn.lib.manager.api.server.obs.TxnExtActionObsContext
import java.util.concurrent.ConcurrentHashMap

class TxnManagerRemoteActions(
    private val manager: TransactionManager,
    private val micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP
) {
    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val actions = ConcurrentHashMap<String, ActionData>()

    init {
        register(TmOnePhaseCommitAction())
        register(TmPrepareCommitAction())
        register(TmCommitPreparedAction())
        register(TmRollbackTxnAction())
        register(TmGetStatusAction())
        register(TmRecoveryCommitAction())
        register(TmRecoveryRollbackAction())
        register(TmCoordinateCommitAction())
        register(TmDisposeTxnAction())
        register(TmExecActionAction())
    }

    fun execute(type: String, data: DataValue, apiVer: Int): DataValue? {
        val action = actions[type] ?: error("Unknown action type '$type'")

        log.debug { "Execute remote action $type with data $data apiVer: $apiVer" }

        val observation = micrometerContext.createObs(TxnExtActionObsContext(type, data, apiVer))

        val res = observation.observe {
            action.action.execute(data.getAsNotNull(action.dataType), apiVer)
        }
        return if (res is Unit) {
            null
        } else {
            DataValue.create(res)
        }
    }

    fun register(action: TxnManagerRemoteAction<*>) {
        val argsType = ReflectUtils.getGenericArgs(action::class.java, TxnManagerRemoteAction::class.java)
        @Suppress("UNCHECKED_CAST")
        actions[action.getType()] = ActionData(
            action as TxnManagerRemoteAction<Any>,
            argsType[0] as Class<Any>
        )
        action.init(manager)
    }

    private class ActionData(
        val action: TxnManagerRemoteAction<Any>,
        val dataType: Class<Any>
    )
}
