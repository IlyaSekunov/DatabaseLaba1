package entities

class Variants(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "id", isAutoIncremented = true))
            addColumn(Column(name = "variant", isUnique = true))
        }
    }

    override fun updateRow(conditions: Map<String, String>, newValues: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val oldVariant = row["variant"] ?: EMPTY_FIELD_VALUE
            val newVariant = newValues["variant"] ?: EMPTY_FIELD_VALUE
            studentsVariantsFull?.updateRow(
                conditions = mapOf("variant" to oldVariant),
                newValues = mapOf("variant" to newVariant)
            )
        }
        return super.updateRow(conditions, newValues)
    }

    override fun deleteRow(conditions: Map<String, String>): Boolean {
        val rowsToBeUpdated = findRowsWhichSatisfy(conditions)
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val variant = row["variant"] ?: EMPTY_FIELD_VALUE
            studentsVariantsFull?.deleteRow(mapOf("variant" to variant))
        }
        return super.deleteRow(conditions)
    }
}