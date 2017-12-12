package company.evo.extfile.robinhood

import io.kotlintest.matchers.*
import io.kotlintest.specs.StringSpec


class RobinHoodHashMapTests : StringSpec() {
    init {
        "test" {
            val map = RobinHoodHashtable(1000)
            map.put(2, 4)
            // map.get(2, 0) shouldBe 4
        }
    }
}