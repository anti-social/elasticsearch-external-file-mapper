package company.evo.persistent.hashmap.straight

import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock

import kotlin.random.Random

import company.evo.io.IOBuffer
import company.evo.persistent.FileDoesNotExistException
import company.evo.persistent.MappedFile
import company.evo.persistent.VersionedDirectory
import company.evo.persistent.VersionedMmapDirectory
import company.evo.persistent.VersionedRamDirectory
import company.evo.persistent.hashmap.Hasher
import company.evo.persistent.hashmap.HasherProvider
import company.evo.rc.RefCounted
import company.evo.rc.use

abstract class StraightHashMapBaseEnv protected constructor(
        protected val dir: VersionedDirectory,
        val collectStats: Boolean
) : AutoCloseable
{
    companion object {
        const val MAX_DISTANCE = 1024

        fun getHashmapFilename(version: Long) = "hashmap_$version.data"
    }

    fun getCurrentVersion() = dir.readVersion()
}

class StraightHashMapROEnv<P: HasherProvider<H>, H: Hasher, W: StraightHashMap, RO: StraightHashMapRO> (
        dir: VersionedDirectory,
        private val mapType: StraightHashMapType<P, H, W, RO>,
        collectStats: Boolean = false
) : StraightHashMapBaseEnv(dir, collectStats) {

    private data class VersionedFile(
            val version: Long,
            val file: RefCounted<MappedFile<IOBuffer>>
    )

    private val lock = ReentrantLock()

    @Volatile
    private var currentFile: VersionedFile = openFile(dir)

    companion object {
        private fun openFile(dir: VersionedDirectory): VersionedFile {
            var version = dir.readVersion()
            while (true) {
                val newFile = tryOpenFile(dir, version)
                if (newFile != null) {
                    return newFile
                }
                val newVersion = dir.readVersion()
                if (newVersion == version) {
                    throw FileDoesNotExistException(Paths.get(getHashmapFilename(version)))
                }
                version = newVersion
            }
        }

        private fun tryOpenFile(dir: VersionedDirectory, version: Long): VersionedFile? {
            return try {
                VersionedFile(
                        version,
                        dir.openFileReadOnly(getHashmapFilename(version))
                )
            } catch (e: FileDoesNotExistException) {
                null
            }
        }
    }

    fun getCurrentMap(): RO {
        var curFile: VersionedFile
        // Retain a map file
        while (true) {
            curFile = currentFile
            if (curFile.file.retain() != null) {
                break
            }
        }

        val version = dir.readVersion()
        if (curFile.version != version) {
            if (lock.tryLock()) {
                try {
                    currentFile = openFile(dir)
                    // Release an old map file
                    curFile.file.release()
                    curFile = currentFile
                    // Retain a map file
                    // we just now created the map file and we are under a lock
                    // so calling retain should be always successful
                    curFile.file.retain() ?:
                            throw IllegalStateException("Somehow the file just opened has been released")
                } finally {
                    lock.unlock()
                }
            }
        }

        // File will be released when closing a hash map
        return mapType.createReadOnly(curFile.version, curFile.file, collectStats)
    }

    override fun close() {
        currentFile.file.release()
        dir.close()
    }
}

class StraightHashMapEnv<P: HasherProvider<H>, H: Hasher, W: StraightHashMap, RO: StraightHashMapRO> private constructor(
        dir: VersionedDirectory,
        val loadFactor: Double,
        private val mapType: StraightHashMapType<P, H, W, RO>,
        private val hasher: Hasher,
        collectStats: Boolean = false
) : StraightHashMapBaseEnv(dir, collectStats) {

    class Builder<P: HasherProvider<H>, H: Hasher, W: StraightHashMap, RO: StraightHashMapRO>(
            private val mapType: StraightHashMapType<P, H, W, RO>
    ) {
        companion object {
            private const val VERSION_FILENAME = "hashmap.ver"
            private const val DEFAULT_INITIAL_ENTRIES = 1024
            private const val DEFAULT_LOAD_FACTOR = 0.75
        }

        var hasher: Hasher = mapType.hasherProvider.run {
            getHasher(defaultHasherSerial)
        }
        fun hasher(serial: Long) = apply {
            hasher = mapType.hasherProvider.run {
                getHasher(serial)
            }
        }

        var initialEntries: Int = DEFAULT_INITIAL_ENTRIES
            private set
        fun initialEntries(maxEntries: Int) = apply {
            if (maxEntries <= 0) {
                throw IllegalArgumentException(
                        "Maximum number of entries cannot be negative or zero"
                )
            }
            this.initialEntries = maxEntries
        }

        var loadFactor: Double = DEFAULT_LOAD_FACTOR
            private set
        fun loadFactor(loadFactor: Double) = apply {
            if (loadFactor <= 0 || loadFactor > 1) {
                throw IllegalArgumentException(
                        "Load factor must be great than zero and less or equal 1"
                )
            }
            this.loadFactor = loadFactor
        }

        var collectStats: Boolean = false
        fun collectStats(collectStats: Boolean) = apply {
            this.collectStats = collectStats
        }

        var useUnmapHack: Boolean = false
        fun useUnmapHack(useUnmapHack: Boolean) = apply {
            this.useUnmapHack = useUnmapHack
        }

        fun open(path: Path): StraightHashMapEnv<P, H, W, RO> {
            val dir = VersionedMmapDirectory.openWritable(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return if (dir.created) {
                create(dir)
            } else {
                openWritable(dir)
            }
        }

        fun openReadOnly(path: Path): StraightHashMapROEnv<P, H, W, RO> {
            val dir = VersionedMmapDirectory.openReadOnly(path, VERSION_FILENAME)
            dir.useUnmapHack = useUnmapHack
            return StraightHashMapROEnv(dir, mapType, collectStats)
        }

        fun createAnonymousDirect(): StraightHashMapEnv<P, H, W, RO> {
            val dir = VersionedRamDirectory.createDirect()
            dir.useUnmapHack = useUnmapHack
            return create(dir)
        }

        fun createAnonymousHeap(): StraightHashMapEnv<P, H, W, RO> {
            val dir = VersionedRamDirectory.createHeap()
            return create(dir)
        }

        private fun create(dir: VersionedDirectory): StraightHashMapEnv<P, H, W, RO> {
            val version = dir.readVersion()
            val filename = getHashmapFilename(version)
            val mapInfo = MapInfo.calcFor(
                    initialEntries, loadFactor, mapType.bucketLayout.size
            )
            dir.createFile(filename, mapInfo.bufferSize).use { file ->
                mapInfo.initBuffer(
                        file.buffer,
                        mapType.keySerializer,
                        mapType.valueSerializer,
                        hasher
                )
            }
            return StraightHashMapEnv(dir, loadFactor, mapType, hasher, collectStats)
        }

        private fun openWritable(dir: VersionedDirectory): StraightHashMapEnv<P, H, W, RO> {
            return StraightHashMapEnv(dir, loadFactor, mapType, hasher, collectStats)
        }
    }

    companion object {
        private val TEMP_SYMBOLS = ('0'..'9').toList() + ('a'..'z').toList() + ('A'..'Z').toList()

        private fun tempFileName(): String {
            val randomPart = (1..8).fold("") { s, _ ->
                s + TEMP_SYMBOLS[Random.nextInt(TEMP_SYMBOLS.size)]
            }
            return ".hashmap_$randomPart.tmp"
        }
    }

    fun openMap(): W {
        val ver = dir.readVersion()
        val mapBuffer = dir.openFileWritable(getHashmapFilename(ver))
        return mapType.createWritable(ver, mapBuffer)
    }

    fun newMap(oldMap: W, maxEntries: Int): W {
        val version = oldMap.version + 1
        val bookmarks = oldMap.loadAllBookmarks()
        val mapInfo = MapInfo.calcFor(
                maxEntries, loadFactor, mapType.bucketLayout.size
        )
        val mapFilename = tempFileName()
        val mappedFile = dir.createFile(
                mapFilename, mapInfo.bufferSize, deleteOnExit = true
        )
        mapInfo.initBuffer(
                mappedFile.get().buffer,
                mapType.keySerializer,
                mapType.valueSerializer,
                hasher
        )
        return mapType.createWritable(version, mappedFile).apply {
            storeAllBookmarks(bookmarks)
        }
    }

    fun copyMap(map: W): W {
        var newMaxEntries = map.size() * 2
        while (true) {
            val newMap = newMap(map, newMaxEntries)
            if (!mapType.copyMap(map, newMap)) {
                // Too many collisions, double number of maximum entries
                newMaxEntries *= 2
                newMap.close()
                continue
            } else {
                return newMap
            }
        }
    }

    fun commit(map: W) {
        val curVersion = dir.readVersion()
        if (map.version <= curVersion) {
            throw IllegalArgumentException("Map have already been committed")
        }
        dir.rename(map.name, getHashmapFilename(map.version))
        dir.writeVersion(map.version)
        dir.deleteFile(getHashmapFilename(curVersion))
    }

    fun discard(map: W) {
        val curVersion = dir.readVersion()
        if (map.version == curVersion) {
            throw IllegalArgumentException("Cannot delete active map")
        }
        dir.deleteFile(map.name)
    }

    override fun close() {
        dir.close()
    }
}
