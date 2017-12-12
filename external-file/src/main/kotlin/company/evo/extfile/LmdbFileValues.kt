package company.evo.extfile

import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.Txn
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.attribute.FileTime


const val MAP_SIZE_DEFAULT = 100_000_000L

class LmdbFileValues(
        private val db: Dbi<ByteBuffer>,
        private val txn: Txn<ByteBuffer>,
        private val keyBuffer: ByteBuffer
) : FileValues {

    class Provider(
            private val config: FileValues.Config,
            override val lastModified: FileTime
    ) : FileValues.Provider {

        companion object {
            val MAX_PENDING_ENTRIES = 1_000_000
        }
        private val dbDir = Files.createTempDirectory("lmdb-")
        private val env = Env.create()
                .setMapSize(config.getLong("map_size", MAP_SIZE_DEFAULT))
                .setMaxDbs(1)
                .open(dbDir.toFile())
        private val db = env.openDbi("external-file-values", DbiFlags.MDB_CREATE)
        private var txn = env.txnWrite()
        private val keyBuffer = ByteBuffer.allocateDirect(env.maxKeySize)
        private val valueBuffer = ByteBuffer.allocateDirect(8)
        private var pendingEntries = 0
        override val sizeBytes: Long
            get() = 0L //db.stat(txn).pageSize.toLong()
        override val values: FileValues
            get() = LmdbFileValues(db, env.txnRead(), keyBuffer)

        override fun put(key: Long, value: Double) {
            keyBuffer.clear()
            keyBuffer.putLong(key).flip()
            valueBuffer.clear()
            valueBuffer.putDouble(value).flip()
            db.put(txn, keyBuffer, valueBuffer)
            pendingEntries += 1
            if (pendingEntries >= MAX_PENDING_ENTRIES) {
                txn.commit()
                txn.close()
                txn = env.txnWrite()
            }
        }

        override fun remove(key: Long) {
            keyBuffer.clear()
            keyBuffer.putLong(key).flip()
            db.delete(keyBuffer)
        }

        fun finalize() {
            println("Temp dir: $dbDir")
            Files.list(dbDir).forEach {
                println("  $it")
            }
            txn.commit()
            txn.close()
        }

        override fun clone(): FileValues.Provider {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }

    override fun get(key: Long, defaultValue: Double): Double {
        keyBuffer.clear()
        keyBuffer.putLong(0L)
        db.get(txn, keyBuffer) ?: return defaultValue
        return txn.`val`().double
    }

    override fun contains(key: Long): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
