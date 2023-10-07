import core.Database
import core.Logger
import entities.Schema
import entities.Table
import java.io.File
import java.util.*
import kotlin.math.max

fun createStudentsTableFromFile(filePath: String) {
    val file = File(filePath)
    if (!file.exists()) {
        Logger.log("File $filePath does not exist")
        return
    }
    if (file.isDirectory) {
        Logger.log("File $filePath is directory but expected to be a file")
        return
    }
    val schema = Database.defaultSchema
    if (schema == null) {
        Logger.log("Declare default schema firstly")
        return
    }
    if (schema.containsTable("students")) {
        Logger.log("Entities.Table students already exists")
        return
    }
    schema.createTable("students")
    val studentsTable = schema.findTable("students")
    if (studentsTable == null) {
        Logger.log("Unable to create a table 'students'")
        return
    }
    file.readLines().forEach {
        val studentFullName = it.split(" ")
        val columnValues = when (studentFullName.size) {
            0 -> {
                Logger.log("Data line cannot be empty")
                return
            }
            1 -> mapOf("name" to studentFullName[0])
            2 -> mapOf("name" to studentFullName[0], "surname" to studentFullName[1])
            3 -> mapOf("name" to studentFullName[0], "surname" to studentFullName[1], "patronymic" to studentFullName[2])
            else -> {
                Logger.log("Incorrect data in a table")
                return
            }
        }
        studentsTable.insertRow(columnValues)
    }
}

fun createVariantsTableFromFile(filePath: String) {
    val file = File(filePath)
    if (!file.exists()) {
        Logger.log("File $filePath does not exist")
        return
    }
    if (file.isDirectory) {
        Logger.log("File $filePath is directory but expected to be a file")
        return
    }
    val schema = Database.defaultSchema
    if (schema == null) {
        Logger.log("Declare default schema firstly")
        return
    }
    if (schema.containsTable("variants")) {
        Logger.log("Entities.Table students already exists")
        return
    }
    schema.createTable("variants")
    val variants = schema.findTable("variants")
    if (variants == null) {
        Logger.log("Unable to create a table 'variants'")
        return
    }
    file.readLines().forEach {
        variants.insertRow(mapOf(
            "variant" to it
        ))
    }
}

fun startLaba() {
    Logger.log("Started laba task...")
    val schema = Database.defaultSchema

    if (schema != null) {
        val studentsTable = schema.findTable("students")
        val variantsTable = schema.findTable("variants")
        if (studentsTable == null || variantsTable == null) {
            Logger.log("Couldn't find tables. Firstly create tables 'students' and 'variants' or load them from files")
            return
        }
        createTableStudentsVariants(schema, studentsTable, variantsTable)
    }
}

fun createTableStudentsVariants(schema: Schema, studentsTable: Table, variantsTable: Table) {
    if (schema.containsTable("students_variants")) return

    schema.createTable("students_variants_full")
    schema.createTable("students_variants")
    val studentsVariantsTable = schema.findTable("students_variants")
    if (studentsVariantsTable == null) {
        Logger.log("Couldn't create table 'students_variants'")
        return
    }

    val studentsRows = studentsTable.rows()
    val variantsRows = variantsTable.rows()
    val setOfPossibleVariants = variantsRows.map { it["id"] }.toMutableList()
    val random = Random()

    for (studentRow in studentsRows) {
        val studentId = studentRow["id"]
        if (setOfPossibleVariants.isEmpty()) {
            setOfPossibleVariants.addAll(variantsRows.map { it["id"] })
        }
        val variantId = setOfPossibleVariants[random.nextInt(setOfPossibleVariants.size)]
        if (studentId != null && variantId != null) {
            studentsVariantsTable.insertRow(mapOf("student_id" to studentId, "variant_id" to variantId))
        }
        setOfPossibleVariants -= variantId
    }
}

fun printTableRows(rows: List<String>) {
    var width = 0
    rows.forEach { line ->
        val splitLine = line.split(Table.TABLE_DATA_SEPARATOR)
        splitLine.forEach {
            width = max(width, it.length)
        }
    }
    ++width
    rows.forEachIndexed { index, line ->
        val lineSplit = line.split(Table.TABLE_DATA_SEPARATOR)
        if (index == 0) {
            for (i in 0..< lineSplit.size * width) print("-")
            println()
            lineSplit.forEach {
                System.out.printf("%${width}s", "${it.split(Table.COLUMN_METADATA_SEPARATOR)[0]}|")
            }
            println()
            for (i in 0..< lineSplit.size * width) print("-")
        } else {
            lineSplit.forEachIndexed { i, s ->
                if (i == 0) println()
                System.out.printf("%${width}s", "$s|")
            }
        }
    }
    println()
}