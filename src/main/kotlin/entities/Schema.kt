package entities

import entities.Table.Companion.FILE_EXTENSION
import config.FILE_PATH_SEPARATOR
import core.Logger
import exceptions.SchemaInitializationException
import config.WORKDIR
import java.io.File

class Schema(filePath: String) {
    private val directory = File(filePath)
    val name: String = filePath.split(FILE_PATH_SEPARATOR).last
    private val tables: MutableList<Table> = ArrayList()

    init {
        Logger.log("Initialization schema '$name'...")
        if (directory.exists()) {
            if (directory.isFile) {
                val message = "Cannot initialize schema from $filePath because it is a file"
                Logger.log(message)
                throw SchemaInitializationException(message)
            }
            directory.walk().forEach {
                if (it.isFile) {
                    when (it.name) {
                        "students$FILE_EXTENSION" -> tables.add(Students(it.absolutePath, this))
                        "variants$FILE_EXTENSION" -> tables.add(Variants(it.absolutePath, this))
                        "students_variants$FILE_EXTENSION" -> tables.add(StudentsVariants(it.absolutePath, this))
                        "students_variants_full$FILE_EXTENSION" -> tables.add(StudentsVariantsFull(it.absolutePath, this))
                    }
                }
            }
        } else {
            directory.mkdir()
        }
        Logger.log("Schema '$name' has been initialized successfully")
    }

    fun createTable(tableName: String) {
        if (tables.find { it.name == tableName } == null) {
            val tablePath = "${directory.absolutePath}$FILE_PATH_SEPARATOR$tableName$FILE_EXTENSION"
            when (tableName) {
                "students" -> tables.add(Students(tablePath, this))
                "variants" -> tables.add(Variants(tablePath, this))
                "students_variants" -> tables.add(StudentsVariants(tablePath, this))
                "students_variants_full" -> tables.add(StudentsVariantsFull(tablePath, this))

            }
        }
    }

    fun findTable(tableName: String) = tables.find { it.name == tableName }

    fun deleteTable(tableName: String) {
        val table = tables.find { it.name == tableName }
        if (table != null) {
            table.delete()
            tables.remove(table)
        } else {
            Logger.log("There is no table '$tableName' in schema '$name'")
        }
    }

    fun delete() {
        deleteAllTables()
        if (directory.deleteRecursively()) {
            Logger.log("Schema '$name' has been deleted successfully")
        } else {
            Logger.log("Couldn't delete schema '$name'")
        }
    }

    private fun deleteAllTables() {
        val iterator = tables.iterator()
        while (iterator.hasNext()) {
            val table = iterator.next()
            table.delete()
            iterator.remove()
        }
        Logger.log("Tables have been deleted successfully")
    }

    fun loadTableFromFile(filePath: String) {
        val inputFile = File(filePath)
        if (!inputFile.exists()) {
            Logger.log("Incorrect path to file")
            return
        }
        if (inputFile.isDirectory) {
            Logger.log("Table must be a file, but found directory")
            return
        }
        if (".${inputFile.extension}" != FILE_EXTENSION) {
            Logger.log("Table must be a file with $FILE_EXTENSION extension")
            return
        }
        val tableName = filePath.split(FILE_PATH_SEPARATOR).last.removeSuffix(FILE_EXTENSION)
        val newTableFile = File("$WORKDIR$FILE_PATH_SEPARATOR$name$FILE_PATH_SEPARATOR$tableName$FILE_EXTENSION")
        inputFile.copyTo(newTableFile)
        try {
            createTable(tableName)
            Logger.log("Table $tableName has been loaded from file $filePath successfully")
        } catch (e: Exception) {
            Logger.log(e.message.toString())
            newTableFile.delete()
        }
    }

    fun copyTo(directoryPath: String) {
        val directory = File(directoryPath)
        if (!directory.exists()) {
            Logger.log("Directory doesn't exist")
            return
        }
        if (directory.isFile) {
            Logger.log("Expected path to directory, not to file")
            return
        }

        val targetDirectory = File("${directory.absolutePath}$FILE_PATH_SEPARATOR$name")
        this.directory.copyRecursively(targetDirectory)
        Logger.log("Schema was copied to directory ${targetDirectory.absolutePath}")
    }

    fun tables() = tables.toList()

    fun containsTable(tableName: String) = findTable(tableName) != null
}