package core

import config.Keywords
import config.WHITE_SPACE
import createStudentsTableFromFile
import createVariantsTableFromFile
import entities.Table
import printTableRows
import startLaba
import java.util.*

object TaskManager : Thread() {

    private fun validateSchema(schema: String): Boolean = Database.containsSchema(schema)

    private fun replaceValuesInsideQuotes(string: String): String {
        val result = StringBuilder()
        val currentWord = StringBuilder()
        var insideQuotes = false
        for (char in string) {
            if (insideQuotes) {
                when (char) {
                    '\'' -> {
                        result.append(currentWord)
                        currentWord.clear()
                        insideQuotes = false
                    }
                    ' ' -> currentWord.append(WHITE_SPACE)
                    else -> currentWord.append(char)
                }
            } else {
                if (char == '\'') {
                    insideQuotes = true
                } else {
                    result.append(char)
                }
            }
        }
        if (insideQuotes) {
            throw Exception("Incorrect sequence of quotes. Check your expression")
        }
        return result.toString()
    }

    private fun extractKeyValuesAfterKeyword(request: List<String>, keyWord: String): Map<String, String> {
        var currentIndex = 0
        while (currentIndex < request.size) {
            if (request[currentIndex++] == keyWord) break
        }
        if (currentIndex == request.size) return emptyMap()

        val result = HashMap<String, String>()
        while (currentIndex < request.size) {
            val currentWord = request[currentIndex++]
            if (Keywords.entries.find { it.name.lowercase(Locale.getDefault()) == currentWord } != null) break
            val keyValue = currentWord.split("=")
            val columnValue = keyValue[1].split(WHITE_SPACE).joinToString(separator = " ")
            result += keyValue[0] to columnValue
        }
        return result
    }

    override fun run() {
        Logger.log("Welcome to database!")
        val scanner = Scanner(System.`in`)
        while (true) {
            val requestString = replaceValuesInsideQuotes(scanner.nextLine().trim())
            val request = requestString.split(" ")
            if (request.isEmpty()) continue
            var currentWord = 0
            when (request[currentWord]) {
                "create" -> {
                    if (++currentWord == request.size) {
                        Logger.log("Incorrect input. Expected keywords: 'schema', 'table' after 'create'")
                        continue
                    }
                    when (request[currentWord]) {
                        "schema" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected schema name after 'schema' but found nothing")
                                continue
                            }
                            Database.loadSchema(request[currentWord])
                        }
                        "table" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected table name after 'table'")
                                continue
                            }
                            if (Database.defaultSchema == null) {
                                Logger.log("Incorrect input. Firstly set default schema")
                                continue
                            }
                            Database.defaultSchema!!.createTable(request[currentWord])
                        }
                        else -> Logger.log("Incorrect word after 'create'. Expected: 'table', 'schema'")
                    }
                }
                "set" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'default' after 'set'")
                        continue
                    }
                    when (request[currentWord]) {
                        "default" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected keyword after 'default' but found nothing")
                                continue
                            }
                            when (request[currentWord]) {
                                "schema" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Incorrect input. Expected schema name after 'schema' but found nothing")
                                        continue
                                    }
                                    val schemaToBeDefault = request[currentWord]
                                    if (!validateSchema(schemaToBeDefault)) {
                                        Logger.log("Entities.Schema '${schemaToBeDefault}' not found")
                                        continue
                                    }
                                    Database.defaultSchema = Database.findSchema(schemaToBeDefault)
                                    Logger.log("Default schema '${Database.defaultSchema?.name}' has been set")
                                }
                                else -> {
                                    Logger.log("Incorrect word. Expected 'schema'")
                                }
                            }
                        }
                        else -> Logger.log("Incorrect input. Expected keyword: 'default'")
                    }
                }
                "update" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'table' after 'update'")
                        continue
                    }
                    when (request[currentWord]) {
                        "table" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected table name after 'table'")
                                continue
                            }
                            if (Database.defaultSchema == null) {
                                Logger.log("There is no default schema. Declare default schema firstly")
                                continue
                            }
                            val tableName = request[currentWord]
                            val schema = Database.defaultSchema

                            if (schema != null) {
                                if (!schema.containsTable(tableName)) {
                                    Logger.log("There is no table $tableName in schema ${schema.name}")
                                    continue
                                }
                                if (++currentWord > request.lastIndex) {
                                    Logger.log("Incorrect input. Expected keyword: 'set'")
                                    continue
                                }
                                when (request[currentWord]) {
                                    "set" -> {
                                        if (++currentWord > request.lastIndex) {
                                            Logger.log("Incorrect input. Expected column_name=new_value after 'set'")
                                            continue
                                        }
                                        val newValues = extractKeyValuesAfterKeyword(request, "set")
                                        if (newValues.isEmpty()) {
                                            Logger.log("List of column=new_value is empty. New values should follow 'set'")
                                            continue
                                        }
                                        val conditions = extractKeyValuesAfterKeyword(request, "where")
                                        if (conditions.isEmpty()) {
                                            Logger.log("There is no conditions. They should follow keyword 'where'")
                                            continue
                                        }
                                        schema.findTable(tableName)?.updateRow(conditions, newValues)
                                    }
                                    else -> Logger.log("Incorrect input. Expected keyword: 'set'")
                                }
                            }
                        }
                        else -> Logger.log("Incorrect input. Expected keyword: 'table'")
                    }
                }
                "delete" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'schema', 'table', 'from' after 'delete")
                        continue
                    }
                    when (request[currentWord]) {
                        "schema" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected schema name after 'schema' keyword")
                                continue
                            }
                            val schemaName = request[currentWord]
                            if (!validateSchema(schemaName)) {
                                Logger.log("There is no schema with name $schemaName")
                                continue
                            }
                            if (Database.defaultSchema?.name == schemaName) {
                                Database.defaultSchema = null
                            }
                            Database.deleteSchema(schemaName)
                        }
                        "table" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected table name after 'table' keyword")
                                continue
                            }
                            if (Database.defaultSchema == null) {
                                Logger.log("There is no default schema. Declare default schema firstly")
                                continue
                            }
                            val tableName = request[currentWord]
                            val schema = Database.defaultSchema
                            schema?.deleteTable(tableName)
                        }
                        "from" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected keyword 'table'")
                                continue
                            }
                            when (request[currentWord]) {
                                "table" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Incorrect input. Expected table name after 'table'")
                                        continue
                                    }
                                    if (Database.defaultSchema == null) {
                                        Logger.log("There is no default schema. Declare default schema firstly")
                                        continue
                                    }

                                    val tableName = request[currentWord]
                                    val schema = Database.defaultSchema
                                    if (schema != null) {
                                        if (!schema.containsTable(tableName)) {
                                            Logger.log("There is no table '$tableName' in schema '${schema.name}'")
                                            continue
                                        }
                                        if (++currentWord > request.lastIndex) {
                                            Logger.log("Incorrect input. Expected keywords: 'where'")
                                            continue
                                        }
                                        when (request[currentWord]) {
                                            "where" -> {
                                                if (++currentWord > request.lastIndex) {
                                                    Logger.log("Incorrect input. Expected column_name=value")
                                                    continue
                                                }
                                                val conditionsToDelete = extractKeyValuesAfterKeyword(request, "where")
                                                schema.findTable(tableName)?.deleteRow(conditionsToDelete)
                                            }
                                            else -> Logger.log("Incorrect input. Expected keyword: 'where'")
                                        }
                                    }
                                }
                                else -> Logger.log("Incorrect input. Expected keywords: 'table'")
                            }
                        }
                        else -> Logger.log("Incorrect input. Expected keywords: 'table', 'schema'")
                    }
                }
                "insert" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'into' after 'insert'")
                        continue
                    }
                    when (request[currentWord]) {
                        "into" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected keyword: 'table', 'schema' after 'into'")
                                continue
                            }
                            when (request[currentWord]) {
                                "table" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Incorrect input. Expected table name after 'table'")
                                        continue
                                    }
                                    if (Database.defaultSchema == null) {
                                        Logger.log("There is no default schema. Declare default schema firstly")
                                        continue
                                    }
                                    val schema = Database.defaultSchema
                                    if (schema != null) {
                                        val tableName = request[currentWord]
                                        if (!schema.containsTable(tableName)) {
                                            Logger.log("There is no table '$tableName' in schema '${schema.name}'")
                                            continue
                                        }
                                        if (++currentWord > request.lastIndex) {
                                            Logger.log("Incorrect input expected keyword 'values' after table name")
                                            continue
                                        }
                                        when (request[currentWord]) {
                                            "values" -> {
                                                val table = schema.findTable(tableName)
                                                val keyValues = extractKeyValuesAfterKeyword(request, "values")
                                                table?.insertRow(keyValues)
                                            }
                                            else -> Logger.log("Incorrect input expected keyword 'values' after table name")
                                        }
                                    }
                                }
                                "schema" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Incorrect input. Expected keyword: 'table' after 'schema'")
                                        continue
                                    }
                                    when (request[currentWord]) {
                                        "table" -> {
                                            if (++currentWord > request.lastIndex) {
                                                Logger.log("Incorrect input. Expected keyword 'from' after 'table'")
                                                continue
                                            }
                                            when (request[currentWord]) {
                                                "from" -> {
                                                    if (++currentWord > request.lastIndex) {
                                                        Logger.log("Incorrect input. Expected keyword 'file' after 'from'")
                                                        continue
                                                    }
                                                    when (request[currentWord]) {
                                                        "file" -> {
                                                            if (++currentWord > request.lastIndex) {
                                                                Logger.log("Incorrect input. Expected file path after 'file'")
                                                                continue
                                                            }
                                                            if (Database.defaultSchema == null) {
                                                                Logger.log("There is no default schema. Declare default schema firstly")
                                                                continue
                                                            }
                                                            val filePath = request[currentWord]
                                                            val schema = Database.defaultSchema
                                                            schema?.loadTableFromFile(filePath)
                                                        }
                                                        else -> Logger.log("Incorrect input. Expected keyword 'file' after 'from'")
                                                    }
                                                }
                                                else -> Logger.log("Incorrect input. Expected keyword 'from' after 'table'")
                                            }
                                        }
                                        else -> Logger.log("Incorrect input. Expected keyword: 'table' after 'schema'")
                                    }
                                }
                                else -> Logger.log("Incorrect input. Expected keyword: 'table', 'schema' after 'into'")
                            }
                        }
                        else -> Logger.log("Incorrect input. Expected keyword: 'into' after insert")
                    }
                }
                "schemas" -> {
                    if (Database.schemas().isEmpty()) {
                        Logger.log("Schemas list is empty")
                        continue
                    }
                    Database.schemas().forEach {
                        Logger.log(it.name)
                    }
                }
                "tables" -> {
                    if (Database.defaultSchema == null) {
                        Logger.log("There is no default schema. Declare default schema firstly")
                        continue
                    }
                    val schema = Database.defaultSchema
                    if (schema != null) {
                        val tablesList = schema.tables()
                        if (tablesList.isEmpty()) {
                            Logger.log("Tables list is empty")
                        } else {
                            tablesList.forEach { Logger.log(it.name) }
                        }
                    }
                }
                "select" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'from' after 'select'")
                        continue
                    }
                    when (request[currentWord]) {
                        "from" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected keyword: 'table' after 'from'")
                                continue
                            }
                            when (request[currentWord]) {
                                "table" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Incorrect input. Expected table name after 'table'")
                                        continue
                                    }
                                    if (Database.defaultSchema == null) {
                                        Logger.log("There is no default schema. Declare default schema firstly")
                                        continue
                                    }
                                    val tableName = request[currentWord]
                                    val schema = Database.defaultSchema
                                    if (schema != null) {
                                        if (!schema.containsTable(tableName)) {
                                            Logger.log("There is no table with name '$tableName' in schema '${schema.name}'")
                                            continue
                                        }

                                        if (currentWord == request.lastIndex) {
                                            val table = schema.findTable(tableName)
                                            if (table != null) {
                                                val rows = mutableListOf(table.columns.joinToString(separator = Table.TABLE_DATA_SEPARATOR) { it.name })
                                                table.rows().forEach {
                                                    rows += it.sequencedValues().joinToString(separator = Table.TABLE_DATA_SEPARATOR)
                                                }
                                                printTableRows(rows)
                                            }
                                        } else {
                                            when (request[++currentWord]) {
                                                "where" -> {
                                                    if (++currentWord > request.lastIndex) {
                                                        Logger.log("Incorrect input. Expected id=value")
                                                        continue
                                                    }
                                                    val conditions = extractKeyValuesAfterKeyword(request, "where")
                                                    val table = schema.findTable(tableName)
                                                    if (table != null) {
                                                        val rows = mutableListOf(table.columns.joinToString(separator = Table.TABLE_DATA_SEPARATOR) { it.name })
                                                        table.findRowsWhichSatisfy(conditions).forEach {
                                                            rows += it.sequencedValues().joinToString(separator = Table.TABLE_DATA_SEPARATOR)
                                                        }
                                                        printTableRows(rows)
                                                    }
                                                }
                                                else -> Logger.log("Incorrect input. Expected keyword: 'where'")
                                            }
                                        }
                                    }
                                }
                                else -> Logger.log("Incorrect input. Expected keyword: 'table' after 'from'")
                            }
                        }
                        else -> Logger.log("Incorrect input. Expected keyword: 'from' after 'select'")
                    }
                }
                "backup" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keywords: 'to', 'from'")
                        continue
                    }
                    when (request[currentWord]) {
                        "to" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected file path to directory")
                                continue
                            }
                            if (Database.defaultSchema == null) {
                                Logger.log("There is no default schema. Declare default schema firstly")
                                continue
                            }
                            val directoryPath = request[currentWord]
                            Database.defaultSchema?.copyTo(directoryPath)
                        }
                        "from" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected file path to schema(directory)")
                                continue
                            }
                            val directoryPath = request[currentWord]
                            DatabaseLoader.load(directoryPath)
                        }
                        else -> Logger.log("Incorrect input. Expected keywords: 'to', 'from'")
                    }
                }
                "default" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected keyword: 'schema' after 'default'")
                        continue
                    }
                    when (request[currentWord]) {
                        "schema" -> Logger.log("Default schema: '${Database.defaultSchema?.name}'")
                        else -> Logger.log("Incorrect input. Expected keyword: 'schema' after 'default'")
                    }
                }
                "laba" -> {
                    if (Database.defaultSchema == null) {
                        Logger.log("There is no default schema. Declare default schema firstly")
                        continue
                    }
                    if (++currentWord > request.lastIndex) {
                        startLaba()
                        continue
                    }
                    when (request[currentWord]) {
                        "students" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Expected 'from' after students")
                                continue
                            }
                            when (request[currentWord]) {
                                "from" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Expected 'file' after 'from'")
                                        continue
                                    }
                                    when (request[currentWord]) {
                                        "file" -> {
                                            if (++currentWord > request.lastIndex) {
                                                Logger.log("Expected file path after 'file'")
                                                continue
                                            }
                                            val filePath = request[currentWord]
                                            createStudentsTableFromFile(filePath)
                                        }
                                        else -> Logger.log("Expected 'file' after 'from'")
                                    }
                                }
                                else -> Logger.log("Expected 'from' after students")
                            }
                        }
                        "variants" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Expected 'from' after variants")
                                continue
                            }
                            when (request[currentWord]) {
                                "from" -> {
                                    if (++currentWord > request.lastIndex) {
                                        Logger.log("Expected 'file' after 'from'")
                                        continue
                                    }
                                    when (request[currentWord]) {
                                        "file" -> {
                                            if (++currentWord > request.lastIndex) {
                                                Logger.log("Expected file path after 'file'")
                                                continue
                                            }
                                            val filePath = request[currentWord]
                                            createVariantsTableFromFile(filePath)
                                        }
                                        else -> Logger.log("Expected 'file' after 'from'")
                                    }
                                }
                                else -> Logger.log("Expected 'from' after variants")
                            }
                        }
                        else -> Logger.log("Expected keyword 'students'")
                    }
                }
                "help" -> {
                    Logger.log("create 'schema'/'table' name")
                    Logger.log("set default 'schema' schema_name")
                    Logger.log("update 'table' table_name [['add column' column_name], ['values' columns_names=values 'where' columns_names=values]]")
                    Logger.log("delete [['schema'/'table' name], ['from' 'table' table_name 'where' column=value]] ")
                    Logger.log("insert into [['schema' 'table' from 'file' file_path], ['table' table_name 'values' columns_names=values]")
                    Logger.log("default 'schema' schema_name")
                    Logger.log("select 'from' 'table' table_name ['where' columns=values]")
                    Logger.log("backup 'to'/'from' file_path")
                    Logger.log("tables")
                    Logger.log("schemas")
                    Logger.log("exit")
                    Logger.log("laba ['students'/'variants' 'from' 'file' file_path]")
                }
                "reset" -> {
                    if (++currentWord > request.lastIndex) {
                        Logger.log("Incorrect input. Expected 'default schema' after 'reset'")
                        continue
                    }
                    when (request[currentWord]) {
                        "default" -> {
                            if (++currentWord > request.lastIndex) {
                                Logger.log("Incorrect input. Expected 'default schema' after 'reset'")
                                continue
                            }
                            Database.defaultSchema = null
                        }
                        else -> Logger.log("Incorrect input. Expected 'default schema' after 'reset'")
                    }
                }
                "exit" -> break
                else -> {
                    Logger.log("Unknown command. Press 'help' to know all available commands")
                }
            }
        }
    }
}