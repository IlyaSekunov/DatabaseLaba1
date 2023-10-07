package core

import config.FILE_PATH_SEPARATOR
import config.WORK_DIR_PATH
import entities.Schema

object Database {
    private val schemas: MutableSet<Schema> = HashSet()
    var defaultSchema: Schema? = null

    fun loadSchema(schemaName: String) {
        if (schemas.find { it.name == schemaName } == null) {
            Logger.log("Loading schema '$schemaName'...")
            schemas.add(Schema("$WORK_DIR_PATH$FILE_PATH_SEPARATOR$schemaName"))
        } else {
            Logger.log("Entities.Schema $schemaName already exists")
        }
    }

    fun deleteSchema(schemaName: String) {
        val schema = schemas.find { it.name == schemaName }
        if (schema == null) {
            Logger.log("There is no schema with name $schemaName")
            return
        }
        Logger.log("Deleting schema '$schemaName'...")
        schema.delete()
        schemas.remove(schema)
        Logger.log("Entities.Schema '$schemaName' has been deleted successfully")
    }

    fun schemas() = schemas.toList()

    fun findSchema(schemaName: String) = schemas.find { it.name == schemaName }

    fun containsSchema(schemaName: String) = schemas.find { it.name == schemaName } != null
}