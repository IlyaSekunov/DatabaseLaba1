package entities

import core.Logger
import java.util.Random

class Students(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "id", isAutoIncremented = true))
            addColumn(Column(name = "name", isPk = true))
            addColumn(Column(name = "surname", isPk = true))
            addColumn(Column(name = "patronymic", isPk = true))
        }
    }

    override fun insertRow(row: Map<String, String>): Boolean {
        if (row["name"] == null || row["surname"] == null) {
            Logger.log("Value for 'name' and 'surname' cannot be empty")
            return false
        }
        if (!super.insertRow(row)) return false
        val variants = schema.findTable("variants")
        if (variants != null) {
            val variantsRows = variants.rows()
            if (variantsRows.isNotEmpty()) {
                val randomVariant = variantsRows[Random().nextInt(variantsRows.size)]
                val variantId = randomVariant["id"] ?: EMPTY_FIELD_VALUE

                if (!schema.containsTable("students_variants")) {
                    schema.createTable("students_variants")
                }
                val studentsVariants = schema.findTable("students_variants")
                studentsVariants?.insertRow(mapOf(
                    "student_id" to (sequence.current - 1).toString(),
                    "variant_id" to variantId
                ))
            }
        }
        return true
    }

    override fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val name = row["name"]
            val surname = row["surname"]
            val patronymic = row["patronymic"]
            val oldFullName = "$name $surname $patronymic"
            val newFullName = "${newValues["name"] ?: name} ${newValues["surname"] ?: surname} ${newValues["patronymic"] ?: patronymic}"
            studentsVariantsFull?.updateRow(
                conditions = mapOf("full_name" to oldFullName),
                newValues = mapOf("full_name" to newFullName)
            )
        }
        return super.updateRow(conditions, newValues)
    }

    override fun deleteRow(conditions: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariants = schema.findTable("students_variants")
        rowsToBeUpdated.forEach { row ->
            val studentId = row["id"] ?: EMPTY_FIELD_VALUE
            studentsVariants?.deleteRow(mapOf("student_id" to studentId))
        }
        return super.deleteRow(conditions)
    }
}