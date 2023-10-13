package entities

import core.Logger

class StudentsVariantsFull(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "full_name", isUnique = true, isPk = true))
            addColumn(Column(name = "path_to_file"))
            addColumn(Column(name = "mark"))
        }
    }

    override fun insertRow(row: Map<String, String>): Boolean {
        val students = schema.findTable("students")
        val variants = schema.findTable("variants")
        if (students == null || variants == null) {
            Logger.log("There are no tables 'students' or 'variants'")
            return false
        }
        val fullName = row["full_name"]
        if (fullName == null) {
            Logger.log("Column 'full_name' cannot be empty")
            return false
        }
        val fullNameSplit = fullName.split(" ")
        val studentConditions = when (fullNameSplit.size) {
            1 -> mapOf("name" to fullNameSplit[0], "surname" to EMPTY_FIELD_VALUE, "patronymic" to EMPTY_FIELD_VALUE)
            2 -> mapOf("name" to fullNameSplit[0], "surname" to fullNameSplit[1], "patronymic" to EMPTY_FIELD_VALUE)
            3 -> mapOf("name" to fullNameSplit[0], "surname" to fullNameSplit[1], "patronymic" to fullNameSplit[2])
            else -> {
                Logger.log("Incorrect value passed to 'full_name'")
                return false
            }
        }
        if (students.findRowsWhichSatisfy(studentConditions).isEmpty()) {
            Logger.log("There is no student $studentConditions")
            return false
        }

        val variant = row["path_to_file"]
        if (variant == null) {
            Logger.log("Column 'path_to_file' cannot be empty")
            return false
        }
        if (variants.findRowsWhichSatisfy(mapOf("path_to_file" to variant)).isEmpty()) {
            Logger.log("There is no variant = '$variant'")
            return false
        }
        return super.insertRow(row)
    }

    override fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        val variant = newValues["path_to_file"]
        if (variant == null) {
            Logger.log("Column 'path_to_file' cannot be empty")
            return false
        }
        val variants = schema.findTable("variants")
        if (variants?.findRowsWhichSatisfy(mapOf("path_to_file" to variant))!!.isEmpty()) {
            Logger.log("There is no variant = '$variant'")
            return false
        }
        return super.updateRow(conditions, newValues)
    }
}