package company.evo.persistent.hashmap

import kotlin.math.max

class BucketLayout(
        val metaSize: Int,
        val keySize: Int,
        val valueSize: Int
) {
    // Let's meta offset always will be zero
    val metaOffset: Int = 0
    val keyOffset: Int
    val valueOffset: Int
    val size: Int

    init {
        val bucketSize: Int
        if (keySize <= valueSize) {
            keyOffset = metaSize + max(0, keySize - metaSize)
            val baseValueOffset = keyOffset + keySize
            valueOffset = baseValueOffset + max(0, valueSize - baseValueOffset)
            bucketSize = valueOffset + valueSize
        } else {
            valueOffset = metaSize + max(0, valueSize - metaSize)
            val baseKeyOffset = valueOffset + valueSize
            keyOffset = baseKeyOffset + max(0, keySize - baseKeyOffset)
            bucketSize = keyOffset + keySize
        }
        val align = maxOf(metaSize, keySize, valueSize)
        size = ((bucketSize - 1) / align + 1) * align
    }

    override fun toString(): String {
        return "BucketLayout<" +
                "metaOffset = $metaOffset, metaSize = $metaSize, " +
                "keyOffset = $keyOffset, keySize = $keySize, " +
                "valueOffset = $valueOffset, valueSize = $valueSize, " +
                "size = $size>"
    }
}
