use num_enum::TryFromPrimitive;

/// MySql server commands
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum Command {
    COM_SLEEP = 0,
    COM_QUIT,
    COM_INIT_DB,
    COM_QUERY,
    COM_FIELD_LIST,
    COM_CREATE_DB,
    COM_DROP_DB,
    COM_REFRESH,
    COM_DEPRECATED_1,
    COM_STATISTICS,
    COM_PROCESS_INFO,
    COM_CONNECT,
    COM_PROCESS_KILL,
    COM_DEBUG,
    COM_PING,
    COM_TIME,
    COM_DELAYED_INSERT,
    COM_CHANGE_USER,
    COM_BINLOG_DUMP,
    COM_TABLE_DUMP,
    COM_CONNECT_OUT,
    COM_REGISTER_SLAVE,
    COM_STMT_PREPARE,
    COM_STMT_EXECUTE,
    COM_STMT_SEND_LONG_DATA,
    COM_STMT_CLOSE,
    COM_STMT_RESET,
    COM_SET_OPTION,
    COM_STMT_FETCH,
    COM_DAEMON,
    COM_BINLOG_DUMP_GTID,
    COM_RESET_CONNECTION,
    COM_END,
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug, TryFromPrimitive)]
#[repr(u32)]
pub enum ColumnDefinitionFlags {
    NOT_NULL_FLAG = 1,
    PRI_KEY_FLAG = 2,
    UNIQUE_KEY_FLAG = 4,
    MULTIPLE_KEY_FLAG = 8,
    BLOB_FLAG = 16,
    UNSIGNED_FLAG = 32,
    ZEROFILL_FLAG = 64,
    BINARY_FLAG = 128,
    ENUM_FLAG = 256,
    AUTO_INCREMENT_FLAG = 512,
    TIMESTAMP_FLAG = 1024,
    SET_FLAG = 2048,
    NO_DEFAULT_VALUE_FLAG = 4096,
    ON_UPDATE_NOW_FLAG = 8192,
    PART_KEY_FLAG = 16384,
    NUM_FLAG = 32768,
    UNIQUE_FLAG = 65536,
    BINCMP_FLAG = 131072,
    GET_FIXED_FIELDS_FLAG = 1 << 18,
    FIELD_IN_PART_FUNC_FLAG = 1 << 19,
    FIELD_IN_ADD_INDEX = 1 << 20,
    FIELD_IS_RENAMED = 1 << 21,
    FIELD_FLAGS_STORAGE_MEDIA = 22,
    FIELD_FLAGS_STORAGE_MEDIA_MASK = 3 << 22,
    FIELD_FLAGS_COLUMN_FORMAT = 24,
    FIELD_FLAGS_COLUMN_FORMAT_MASK = 3 << 24,
    FIELD_IS_DROPPED = 1 << 26,
    EXPLICIT_NULL_FLAG = 1 << 27,
    GROUP_FLAG = 1 << 28,
    NOT_SECONDARY_FLAG = 1 << 29,
    FIELD_IS_INVISIBLE = 1 << 30,
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#aba06d1157f6dee3f20537154103c91a1
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Eq, PartialEq, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ResultSetMetadata {
    RESULTSET_METADATA_NONE = 0,
    RESULTSET_METADATA_FULL,
}
