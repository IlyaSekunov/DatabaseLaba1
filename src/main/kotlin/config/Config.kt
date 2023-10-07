package config

import java.io.File
import java.nio.file.Paths

const val WHITE_SPACE = "%20"
val CURRENT_PATH = Paths.get("").toAbsolutePath().toString()
const val WORKDIR = "workdir"
val FILE_PATH_SEPARATOR = File.separatorChar
val WORK_DIR_PATH = "$CURRENT_PATH$FILE_PATH_SEPARATOR$WORKDIR"

enum class Keywords {
    SELECT,
    CREATE,
    UPDATE,
    DELETE,
    BACKUP,
    FROM,
    VALUES,
    WHERE,
    SCHEMA,
    TABLE,
    DEFAULT,
    SET,
    ADD,
    COLUMN
}