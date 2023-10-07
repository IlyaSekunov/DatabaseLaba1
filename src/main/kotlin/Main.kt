import core.DatabaseLoader
import core.TaskManager

fun main() {
    DatabaseLoader.load()
    TaskManager.start()
}