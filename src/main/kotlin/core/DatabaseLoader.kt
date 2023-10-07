package core

import config.FILE_PATH_SEPARATOR
import config.WORK_DIR_PATH
import java.io.File

object DatabaseLoader {
    fun load() {
        Logger.log("Scanning directory $WORK_DIR_PATH...")
        val workdir = File(WORK_DIR_PATH)
        if (workdir.exists()) {
            File(WORK_DIR_PATH).walk().forEach { file ->
                if (file.isDirectory && file.absolutePath != WORK_DIR_PATH) {
                    Database.loadSchema(file.name)
                }
            }
        } else {
            workdir.mkdir()
        }
        Logger.log("Loading completed")
    }

    fun load(directoryPath: String) {
        Logger.log("Scanning directory $directoryPath...")

        val directory = File(directoryPath)
        if (!directory.exists()) {
            Logger.log("Incorrect path to directory")
            return
        }
        if (directory.isFile) {
            Logger.log("Expected path to directory not to file")
            return
        }

        if (Database.defaultSchema != null) {
            directory.walk().forEach {
                if (it.isDirectory) {
                    if (it.absolutePath != directory.absolutePath) {
                        throw Exception("Entities.Schema cannot contain another schema")
                    }
                } else {
                    Database.defaultSchema!!.loadTableFromFile(it.absolutePath)
                }
            }
        } else {
            directory.copyRecursively(File("$WORK_DIR_PATH$FILE_PATH_SEPARATOR${directory.name}"))
            Database.loadSchema(directory.name)
            Logger.log("Loading completed")
        }
    }
}