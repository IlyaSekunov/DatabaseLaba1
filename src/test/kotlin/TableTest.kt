import entities.Table
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TableTest {
    @Test
    fun columnToStringTest() {
        val column1 = Table.Column("test")
        val column2 = Table.Column("test", isUnique = true)
        val column3 = Table.Column("test", isUnique = true, isPk = true)
        val column4 = Table.Column("test", isPk = true)
        assertEquals(column1.toString(), "test+")
        assertEquals(column2.toString(), "test+u")
        assertEquals(column3.toString(), "test+upk")
        assertEquals(column4.toString(), "test+pk")
    }

    fun rowAsStringTest() {

    }
}