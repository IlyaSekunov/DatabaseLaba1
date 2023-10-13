package entities

class Variants(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "id", isAutoIncremented = true))
            addColumn(Column(name = "path_to_file", isUnique = true))
        }
    }

    override fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val oldVariant = row["path_to_file"] ?: EMPTY_FIELD_VALUE
            val newVariant = newValues["path_to_file"] ?: EMPTY_FIELD_VALUE
            studentsVariantsFull?.updateRow(
                conditions = mapOf("path_to_file" to oldVariant),
                newValues = mapOf("path_to_file" to newVariant)
            )
        }
        return super.updateRow(conditions, newValues)
    }

    override fun deleteRow(conditions: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val variant = row["path_to_file"] ?: EMPTY_FIELD_VALUE
            studentsVariantsFull?.updateRow(
                conditions = mapOf("path_to_file" to variant),
                newValues = mapOf("path_to_file" to EMPTY_FIELD_VALUE)
            )
        }
        return super.deleteRow(conditions)
    }
}