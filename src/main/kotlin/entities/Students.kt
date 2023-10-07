package entities

class Students(filePath: String, schema: Schema) : Table(filePath, schema) {
    init {
        if (columns.isEmpty()) {
            addColumn(Column(name = "id", isAutoIncremented = true))
            addColumn(Column(name = "name", isPk = true))
            addColumn(Column(name = "surname", isPk = true))
            addColumn(Column(name = "patronymic", isPk = true))
        }
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
        val studentsVariantsFull = schema.findTable("students_variants_full")
        rowsToBeUpdated.forEach { row ->
            val name = row["name"]
            val surname = row["surname"]
            val patronymic = row["patronymic"]
            val fullName = "$name $surname $patronymic"
            studentsVariantsFull?.deleteRow(mapOf("full_name" to fullName))
        }
        return super.deleteRow(conditions)
    }
}