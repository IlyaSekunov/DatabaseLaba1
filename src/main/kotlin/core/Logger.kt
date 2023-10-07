package core

import java.io.PrintStream

object Logger {
    var outputStream: PrintStream = System.out
    fun log(message: String) = outputStream.println(message)
}