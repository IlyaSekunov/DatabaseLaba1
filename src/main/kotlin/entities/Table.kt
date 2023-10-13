package entities

import config.FILE_PATH_SEPARATOR
import core.Logger
import exceptions.TableInitializationException
import java.io.File
import java.lang.StringBuilder
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap
import kotlin.math.max

open class Table(filePath: String, val schema: Schema) {
    val file = File(filePath)
    val name = filePath.split(FILE_PATH_SEPARATOR).last.removeSuffix(FILE_EXTENSION)
    val columns = ArrayList<Column>()
    val sequence = Sequence()

    companion object {
        const val EMPTY_FIELD_VALUE = "null"
        const val COLUMN_METADATA_SEPARATOR = "+"
        const val TABLE_DATA_SEPARATOR = "|"
        const val FILE_EXTENSION = ".txt"
    }

    init {
        if (file.exists()) {
            if (file.isDirectory) {
                val message = "Cannot initialize table '$name' from ${file.absolutePath} because it is a directory"
                Logger.log(message)
                throw TableInitializationException(message)
            }
            file.bufferedReader().use { bufferedReader ->
                if (!bufferedReader.ready()) {
                    Logger.log("Table '$name' has been initialized successfully")
                } else {
                    val columnsInFile = bufferedReader.readLine().split(TABLE_DATA_SEPARATOR)
                    columnsInFile.forEach {
                        val columnNameSplitByMetadataSeparator = it.split(COLUMN_METADATA_SEPARATOR)
                        if (columnNameSplitByMetadataSeparator.isEmpty() || columnNameSplitByMetadataSeparator.size == 1) {
                            val message = "Cannot initialize table '$name' from ${file.absolutePath} because of incorrect columns format"
                            Logger.log(message)
                            throw TableInitializationException(message)
                        }
                        val columnName = columnNameSplitByMetadataSeparator[0]
                        val columnMetadata = columnNameSplitByMetadataSeparator[1]
                        val column = Column(columnName)
                        if (columnMetadata.contains("u")) {
                            column.isUnique = true
                        }
                        if (columnMetadata.contains("pk")) {
                            column.isPk = true
                        }
                        if (columnMetadata.contains("a")) {
                            column.isAutoIncremented = true
                        }
                        columns += column
                    }
                    var maxRowAuto = 0
                    val indexOfColumnAutoIncrement = columns.indexOfFirst { it.isAutoIncremented }
                    if (indexOfColumnAutoIncrement != -1) {
                        bufferedReader.forEachLine {
                            maxRowAuto = max(maxRowAuto, it.split(TABLE_DATA_SEPARATOR)[indexOfColumnAutoIncrement].toInt())
                        }
                        sequence.current = maxRowAuto + 1
                    }
                    Logger.log("Table '$name' has been initialized successfully")
                }
            }
        } else {
            file.createNewFile()
            Logger.log("Table '$name' has been initialized successfully")
        }
    }

    /**
     * Returns true if row has been inserted successfully and false if not.
     * Accept row as Map<String, String> where key is a column and value is a value for its column
     **/
    open fun insertRow(row: Map<String, String>): Boolean {
        if (row.isEmpty()) {
            Logger.log("Row to be inserted cannot be empty")
            return false
        }
        val rowToInsert = LinkedHashMap<String, String>(row)
        columns.forEach { column ->
            if (!column.isAutoIncremented) {
                rowToInsert[column.name] = row[column.name] ?: EMPTY_FIELD_VALUE
            } else {
                rowToInsert[column.name] = sequence.current.toString()
            }
        }
        if (violateConstraints(rowToInsert)) {
            Logger.log("Unable to insert new row because it violates table constraints")
            return false
        }
        val newRowAsString = rowAsString(rowToInsert)
        file.appendText("\n$newRowAsString")
        sequence.current++
        Logger.log("Row $row has been inserted successfully")
        return true
    }

    /**
     * Updates all rows in the table which satisfies passed conditions.
     * Returns true if table has been updated successfully and false if not
     * **/
    open fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        if (!isValidColumnValue(newValues.values)) {
            Logger.log("Values cannot contain symbol '$TABLE_DATA_SEPARATOR'")
            return false
        }
        val rowsSatisfiesConditions = findRowsWhichSatisfy(conditions)
        if (rowsSatisfiesConditions.isEmpty()) {
            Logger.log("0 rows have been updated")
            return false
        }
        val fileLines = file.readLines().toMutableList()
        for ((index, line) in fileLines.withIndex()) {
            if (index == 0) continue
            val rowAsMap = rowAsMap(line).toMutableMap()
            if (rowsSatisfiesConditions.contains(rowAsMap)) {
                var isConstraintColumnUpdated = false
                newValues.forEach { newValue ->
                    if (rowAsMap.containsKey(newValue.key)) {
                        val columnToBeUpdated = columns.find { it.name == newValue.key }
                        if (columnToBeUpdated != null && (columnToBeUpdated.isUnique || columnToBeUpdated.isPk)) {
                            isConstraintColumnUpdated = true
                        }
                        rowAsMap[newValue.key] = newValue.value
                    }
                }
                if (isConstraintColumnUpdated && violateConstraints(rowAsMap)) {
                    Logger.log("Row $rowAsMap violates table constraints")
                    continue
                }
                fileLines[index] = rowAsString(rowAsMap)
                Logger.log("Row ${rowAsMap(line)} has been updated successfully")
            }
        }
        file.delete()
        fileLines.forEachIndexed{ index, line ->
            if (index == 0) file.appendText(line)
            else file.appendText("\n$line")
        }
        return true
    }

    /**
     * Deletes all rows which satisfy conditions.
     * Returns true if at least 1 row has been deleted and false if not
     * **/
    open fun deleteRow(conditions: Map<String, String>): Boolean {
        val linesToBeDeleted = findRowsWhichSatisfy(conditions)
        if (linesToBeDeleted.isEmpty()) {
            Logger.log("Found 0 rows satisfies conditions: $conditions")
            return false
        }
        val fileLines = file.readLines().toMutableList()
        linesToBeDeleted.forEach { lineToDelete ->
            fileLines.remove(rowAsString(lineToDelete))
            Logger.log("Row $lineToDelete has been deleted successfully")
        }
        file.delete()
        fileLines.forEachIndexed { index, line ->
            if (index == 0) file.appendText(line)
            else file.appendText("\n$line")
        }
        return true
    }

    /**
     * Returns list of Map<String, String> where each element of list represents a single row.
     * Row are represented as Map<String, String> where keys are columns and values are values for its columns
     * Accept Map<String, String> which resulting list must contain rows which contains keys and values passed to this map.
     * **/
    fun findRowsWhichSatisfy(conditions: Map<String, String>): List<SequencedMap<String, String>> {
        val rows = ArrayList<SequencedMap<String, String>>()
        file.bufferedReader().use { bufferedReader ->
            if (bufferedReader.ready()) {
                bufferedReader.readLine()
                while (bufferedReader.ready()) {
                    val fileLine = bufferedReader.readLine()
                    val fileLineSplit = fileLine.split(TABLE_DATA_SEPARATOR)
                    var allConditionsAreTrue = true
                    for ((index, column) in columns.withIndex()) {
                        if (conditions.contains(column.name)) {
                            val columnValue = fileLineSplit[index]
                            if (columnValue != conditions[column.name]) {
                                allConditionsAreTrue = false
                                break
                            }
                        }
                    }
                    if (allConditionsAreTrue) {
                        rows += rowAsMap(fileLine)
                    }
                }
            }
        }
        return rows
    }

    /**
     * Returns row as string matching table format
     * Accept row as Map<String, String> where key is column name and value is a value for its column
     **/
    fun rowAsString(row: Map<String, String>): String {
        val result = StringBuilder()
        columns.forEachIndexed { index, column ->
            if (index != 0) result.append(TABLE_DATA_SEPARATOR)
            result.append(row[column.name] ?: "null")
        }
        return result.toString()
    }

    /**
     * Returns row as Map<String, String> where keys are columns and values are values of its columns.
     * Accept row as String where value for each column separates with TABLE_DATA_SEPARATOR.
     **/
    fun rowAsMap(row: String): SequencedMap<String, String> {
        val rowSplit = row.split(TABLE_DATA_SEPARATOR)
        if (rowSplit.size != columns.size) {
            Logger.log("Row does not match to columns")
            return LinkedHashMap()
        }
        val result = LinkedHashMap<String, String>()
        columns.forEachIndexed { index, column ->
            result[column.name] = rowSplit[index]
        }
        return result
    }

    /**
     * Returns true if row in argument violate table constraints.
     * Accept row as Map<String, String> where key is a column name and values is a value to be checked
     * If value for particular column is not provided compare with EMPTY_FIELD_VALUE
    **/
    fun violateConstraints(row: Map<String, String>): Boolean {
        val countOfColumnsPrimaryKeys = columns.count { it.isPk }
        file.bufferedReader().use { bufferedReader ->
            if (bufferedReader.ready()) {
                bufferedReader.readLine()
                while (bufferedReader.ready()) {
                    var countOfPrimaryKeysViolated = 0
                    val fileLine = bufferedReader.readLine().split(TABLE_DATA_SEPARATOR)
                    columns.forEachIndexed { columnNumber, column ->
                        val valueToBeChecked = row[column.name] ?: EMPTY_FIELD_VALUE
                        if (column.isUnique && valueToBeChecked == fileLine[columnNumber]) {
                            return true
                        }
                        if (column.isPk && valueToBeChecked == fileLine[columnNumber]) ++countOfPrimaryKeysViolated
                    }
                    if (countOfColumnsPrimaryKeys != 0 && countOfColumnsPrimaryKeys == countOfPrimaryKeysViolated) {
                        return true
                    }
                }
            }
        }
        return false
    }

    /**
     * Add column to the table. Returns true if column added successfully and false in opposite
    * */
    fun addColumn(column: Column): Boolean {
        if (columns.contains(column)) {
            Logger.log("Column '${column.name} already exists'")
            return false
        }
        if (columns.isNotEmpty()) {
            file.delete()
        }
        columns += column
        file.appendText(columns.joinToString(separator = TABLE_DATA_SEPARATOR))
        return true
    }

    fun delete() {
        if (file.delete()) {
            Logger.log("Table '$name' has been deleted successfully")
        } else {
            Logger.log("Couldn't delete table '$name'")
        }
    }

    fun isValidColumnValue(columnValue: String) = !columnValue.contains(TABLE_DATA_SEPARATOR)
    fun isValidColumnValue(columnsValues: Collection<String>) = columnsValues.all { isValidColumnValue(it) }

    fun rows(): List<SequencedMap<String, String>> {
        val result = ArrayList<SequencedMap<String, String>>()
        file.readLines().forEachIndexed { index, line ->
            if (index != 0) {
                val map = LinkedHashMap<String, String>()
                val rowValues = line.split(TABLE_DATA_SEPARATOR)
                columns.forEachIndexed { columnIndex, column ->
                    map[column.name] = rowValues[columnIndex]
                }
                result += map
            }
        }
        return result
    }

    class Column(
        var name: String,
        var isUnique: Boolean = false,
        var isPk: Boolean = false,
        var isAutoIncremented: Boolean = false
    ) {
        override fun equals(other: Any?) = name == other
        override fun hashCode() = name.hashCode()
        override fun toString(): String {
            return "${name}$COLUMN_METADATA_SEPARATOR" +
                    (if (isUnique) "u" else "") +
                    (if (isPk) "pk" else "") +
                    (if (isAutoIncremented) "a" else "")
        }
    }
}