module DBAPI

using DataStreams

abstract type AbstractConnection end
abstract type AbstractCursor end

abstract type AbstractDBSource end
abstract type AbstractDBSink end

abstract type AbstractDBError <: Exception end

struct DBError <: AbstractDBError
    msg::String
end
struct DBImplementationError <: AbstractDBError
    msg::String
end

Base.showerror(io::IO, e::T) where {T<:AbstractDBError} = print(io, T, ": " * e.msg)

"""
*DBAPI*

    isopen

Check whether a connection is open.  Note that this extends `Base.isopen`.

## **Required** Methods:
- `isopen(::AbstractConnection)`
"""
Base.isopen(::AbstractConnection) = false

"""
*DBAPI*

    commit

Commit transaction to a database.

## Optional Methods:
- `commit(::AbstractConnection)`
"""
function commit end

"""
*DBAPI*

    connection

Obtain the `AbstractConnection` associated with the argument.

## **Required** Methods:
- `connection(::AbstractCursor)`
"""
function connection end

"""
*DBAPI*

    cursor

Obtain the cursor associated with the argument.

## Optional Methods:
- `cursor(::AbstractConnection)`
"""
function cursor end

"""
*DBAPI*

    execute!

Execute a database transaction.

## **Required** Methods:
- `execute!(::AbstractCursor, query::AbstractString)`
"""
function execute! end

"""
*DBAPI*

    rollback!

Cause the connection to reset any pending transactions.

## Optional Methods:
- `rollback!(::AbstractConnection)`
"""
function rollback! end

"""
*DBAPI*

    source

Obtain a DataStreams source from the argument.

## **Required** Methods:
- `source(::AbstractCursor)`
"""
function source end

"""
*DBAPI*

    close

Close a connection object.  Note that this extends `Base.close`.

## **Required** Methods:
- `close(::AbstractConnection)`

## Optional Methods:
- `close(::AbstractCursor)`
"""
function Base.close(cnxn::AbstractConnection)
    throw(DBImplementationError("`close` not implemented for connection $cnxn."))
end
Base.close(csr::AbstractCursor) = close(connection(csr))

"""
*DBAPI*

    load(::Type{T}, src)
    load(::Type{T}, csr::AbstractCursor, q::AbstractString)

Load data from a database into a new object of type `T` that implements the DataStreams sink
interface.  `src` can be any object that implements the DataStreams source interface, or
a cursor from which a source can be obtained with the `source` function.
"""
load(::Type{T}, src::AbstractDBSource) where {T} = Data.close!(Data.stream!(src, T))
load(::Type{T}, csr::AbstractCursor) where {T} = load(T, source(csr))
function load(::Type{T}, csr::AbstractCursor, q::AbstractString) where {T}
    execute!(csr, q)
    load(T, csr)
end


export cursor, commit, connection, execute!, rowcount, DBError, DBImplementationError,
    rollback, load


end # module
