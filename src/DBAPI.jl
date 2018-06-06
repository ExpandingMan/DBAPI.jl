__precompile__(true)

module DBAPI

include("base.jl")
include("arrays.jl")

export ColumnarArrayInterface
export cursor,
    execute!,
    executemany!,
    commit,
    rollback,
    rows,
    columns,
    connection,
    fetchintoarray!,
    fetchintorows!,
    fetchintocolumns!,
    DatabaseFetcher,
    interface,
    DatabaseInterface,
    DatabaseError,
    DatabaseConnection,
    DatabaseCursor,
    FixedLengthDatabaseCursor,
    DatabaseQuery,
    ParameterQuery,
    MultiparameterQuery,
    SimpleStringQuery,
    StringParameterQuery,
    StringMultiparameterQuery,
    DatabaseQueryError


end # module
