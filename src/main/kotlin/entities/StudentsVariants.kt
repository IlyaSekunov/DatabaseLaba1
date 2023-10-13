package entities

import core.Logger

class StudentsVariants(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "student_id", isPk = true, isUnique = true))
            addColumn(Column(name = "variant_id"))
        }
    }

    override fun insertRow(row: Map<String, String>): Boolean {
        if (isValidValues(row["student_id"], row["variant_id"]) && super.insertRow(row)) {
            val studentsVariantsFull = schema.findTable("students_variants_full")
            val students = schema.findTable("students")
            val variants = schema.findTable("variants")
            if (students == null || variants == null) return false
            val student = students.findRowsWhichSatisfy(mapOf(
                "id" to (row["student_id"] ?: EMPTY_FIELD_VALUE)
            ))[0]
            val variant = variants.findRowsWhichSatisfy(mapOf(
                "id" to (row["variant_id"] ?: EMPTY_FIELD_VALUE)
            ))[0]
            return studentsVariantsFull?.insertRow(mapOf(
                "full_name" to "${student["name"]} ${student["surname"]} ${student["patronymic"]}",
                "path_to_file" to (variant["path_to_file"] ?: EMPTY_FIELD_VALUE)
            )) ?: false
        }
        return false
    }

    override fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        val students = schema.findTable("students")
        val variants = schema.findTable("variants")
        val studentsVariantsFull = schema.findTable("students_variants_full")
        if (students == null || variants == null || studentsVariantsFull == null) {
            Logger.log("There aren't table 'students', 'variants' or 'students_variants_full'")
            return false
        }
        if (newValues.containsKey("student_id")) {
            Logger.log("Column 'student_id' cannot be changed")
            return false
        }
        val linesToBeUpdated = findRowsWhichSatisfy(conditions)
        linesToBeUpdated.forEach { line ->
            val studentId = line["student_id"]
            if (studentId != null) {
                val student = students.findRowsWhichSatisfy(mapOf("id" to studentId))[0]
                val studentFullName = "${student["name"]} ${student["surname"]} ${student["patronymic"]}"
                val newVariantId = newValues["variant_id"]
                if (newVariantId == null) {
                    Logger.log("Column 'variant_id' cannot be empty")
                    return false
                }
                val newVariants = variants.findRowsWhichSatisfy(mapOf("id" to newVariantId))
                if (newVariants.isEmpty()) {
                    Logger.log("Variant with id $newVariantId not found")
                    return false
                }
                val newVariant = variants.findRowsWhichSatisfy(mapOf("id" to newVariantId))[0]
                val newVariantName = newVariant["path_to_file"] ?: EMPTY_FIELD_VALUE
                studentsVariantsFull.updateRow(
                    conditions = mapOf("full_name" to studentFullName),
                    newValues = mapOf("path_to_file" to newVariantName)
                )
            }
        }
        return super.updateRow(conditions, newValues)
    }

    override fun deleteRow(conditions: Map<String, String>): Boolean {
        val students = schema.findTable("students")
        val variants = schema.findTable("variants")
        val studentsVariantsFull = schema.findTable("students_variants_full")
        if (students == null || variants == null || studentsVariantsFull == null) {
            Logger.log("There aren't table 'students', 'variants' or 'students_variants_full'")
            return false
        }
        val linesToBeDeleted = findRowsWhichSatisfy(conditions)
        linesToBeDeleted.forEach {
            val student = students.findRowsWhichSatisfy(mapOf("id" to (it["student_id"] ?: EMPTY_FIELD_VALUE)))[0]
            studentsVariantsFull.deleteRow(mapOf(
                "full_name" to "${student["name"]} ${student["surname"]} ${student["patronymic"]}"
            ))
        }
        return super.deleteRow(conditions)
    }

    private fun isValidValues(studentId: String?, variantId: String?): Boolean {
        val students = schema.findTable("students")
        val variants = schema.findTable("variants")
        if (students == null || variants == null) {
            Logger.log("There aren't table 'students' or 'variants'")
            return false
        }
        if (students.findRowsWhichSatisfy(mapOf(
                "id" to (studentId ?: EMPTY_FIELD_VALUE)
        )).isEmpty()) {
            Logger.log("There is no student with 'id'=$studentId")
            return false
        }
        if (variants.findRowsWhichSatisfy(mapOf(
                "id" to (variantId ?: EMPTY_FIELD_VALUE)
        )).isEmpty()) {
            Logger.log("There is no variant with 'id'=$variantId")
            return false
        }
        return true
    }
}