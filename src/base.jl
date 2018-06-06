

abstract type DatabaseInterface end
abstract type DatabaseError{T<:DatabaseInterface} <: Exception end
abstract type DatabaseConnection{T<:DatabaseInterface} end
abstract type DatabaseCursor{T<:DatabaseInterface} end
abstract type FixedLengthDatabaseCursor{T} <: DatabaseCursor{T} end

Base.ndims(cursor::FixedLengthDatabaseCursor) = 2

abstract type DatabaseQuery end

abstract type ParameterQuery <: DatabaseQuery end

abstract type MultiparameterQuery <: ParameterQuery end

struct SimpleStringQuery{T<:AbstractString} <: DatabaseQuery
    query::T
end

struct StringParameterQuery{T<:AbstractString, S} <: ParameterQuery
    query::T
    params::S
end

struct StringMultiparameterQuery{T<:AbstractString, S} <: MultiparameterQuery
    query::T
    param_list::S
end

const StringQuery = Union{SimpleStringQuery,StringParameterQuery,StringMultiparameterQuery}

function Base.show(io::IO, connection::DatabaseConnection)
    print(io, typeof(connection), "(closed=$(!isopen(connection)))")
end

function Base.show(io::IO, cursor::DatabaseCursor)
    print(io, typeof(cursor), "(", connection(cursor), ")")
end

"""
Returns the interface type for any database object.
"""
interface(database_object::Union{DatabaseCursor{T}, DatabaseConnection{T}, DatabaseError{T}}
         ) where {T<:DatabaseInterface} = T

"""
If this error is thrown, a driver has not implemented a required function
of this interface.
"""
struct NotImplementedError{T<:DatabaseInterface} <: DatabaseError{T} end

function Base.showerror(io::IO, e::NotImplementedError{T}) where {T<:DatabaseInterface}
    print(io, T, " does not implement this required DBAPI feature")
end

"""
If this error is thrown, a user has attempted to use an optional function
of this interface which the driver does not implement.
"""
struct NotSupportedError{T<:DatabaseInterface} <: DatabaseError{T} end

function Base.showerror(io::IO, e::NotSupportedError{T}) where {T<:DatabaseInterface}
    print(io, T, " does not support this optional DBAPI feature")
end

"""
If this error is thrown, an error occured while processing this database query.
"""
struct DatabaseQueryError{T<:DatabaseInterface, S<:DatabaseQuery} <: DatabaseError{T}
    interface::Type{T}
    query::S
end

function Base.showerror(io::IO, e::DatabaseQueryError{T}) where {T<:DatabaseInterface}
    print(io, "An error occured while processing this query:\n", e.query)
end

"""
Constructs a database connection.

Returns `connection::DatabaseConnection`.
"""
connect(::Type{T}, args...; kwargs...) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Close the connection now (rather than when the finalizer is called).

Any further attempted operations on the connection or its cursors will throw a
subtype of DatabaseError.

Closing a connection without committing will cause an implicit rollback to be
performed.

Returns `nothing`.
"""
close(conn::DatabaseConnection{T}) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Returns true if the connection is open and not broken.

Returns `Bool`
"""
isopen(conn::DatabaseConnection{T}) where {T<:DatabaseInterface = throw(NotImplementedError{T}())

"""
Commit any pending transaction to the database.

Dataase drivers that do not support transactions should implement this
function with no body.

Returns `nothing`.
"""
commit(conn::DatabaseConnection{T}) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Roll back to the start of any pending transaction.

Database drivers that do not support transactions may not implement this
function.

Returns `nothing`.
"""
rollback(conn::DatabaseConnection{T}) where {T<:DatabaseInterface} = throw(NotSupportedError{T}())

"""
Constructs a database connection, runs `func` on that connection, and ensures the
connection is closed after `func` completes or errors.

Returns the result of calling `func`.
"""
function connect(func::Function, ::Type{T}, args...; kwargs...) where {T<:DatabaseInterface}
    conn = connect(T, args...; kwargs...)
    try
        return func(conn)
    finally
        try
            close(conn)
        catch e
            @warn(e)
        end
    end
end

"""
Create a new database cursor.

If the database does not implement cursors, the driver must implement a cursor
object which emulates cursors to the extent required by the interface.

Some drivers may implement multiple cursor types, but all must follow the
`DatabaseCursor` interface. Additional arguments may be given to the
driver's implementation of `cursor` but this method must be implemented with
reasonable defaults.

Returns `DatabaseCursor{T}`.
"""
cursor(conn::DatabaseConnection{T}) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Return the corresponding connection for a given cursor.

Returns `DatabaseConnection{T}`.
"""
connection(cursor::DatabaseCursor{T}) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Run a query on a database.

The results of the query are not returned by this function but are accessible
through the cursor.

`query` can be any subtype of `DatabaseQuery`. There are some query types
designed to work for many databases (e.g. `SimpleStringQuery`,
`StringParameterQuery`, `StringMultiparameterQuery`) and it is suggested that
drivers which support queries in the form of strings implement this method
for those query types. However, it is only required that some query type be
supported.

Returns `nothing`.
"""
function execute!(cursor::DatabaseCursor{T}, query::DatabaseQuery) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

function execute!(cursor::DatabaseCursor{T}, query::SimpleStringQuery) where {T<:DatabaseInterface}
    throw(NotSupportedError{T}())
end

function execute!(cursor::DatabaseCursor{T}, query::StringParameterQuery) where {T<:DatabaseInterface}
    throw(NotSupportedError{T}())
end

function execute!(cursor::DatabaseCursor{T}, query::AbstractString) where {T<:DatabaseInterface}
    execute!(cursor, SimpleStringQuery(query))
end

function execute!(cursor::DatabaseCursor{T}, query::AbstractString, params) where {T<:DatabaseInterface}
    execute!(cursor, StringParameterQuery(query, params))
end

"""
Run a query on a database multiple times with different parameters.

The results of the queries are not returned by this function. The result of
the final query run is accessible by the cursor.

`query` can be any subtype of `MultiparameterQuery`. A `MultiparameterQuery`
typically contains an iterable of iterables of parameters and causes a query to
be executed on each parameter set.

Returns `nothing`.
"""
function execute!(cursor::DatabaseCursor{T}, query::MultiparameterQuery) where {T<:DatabaseInterface}
    throw(NotSupportedError{T}())
end

"""
Create a row iterator.

This method should return an instance of an iterator type which returns one row
on each iteration. Each row should be returned as a Tuple{...} with as much
type information in the Tuple{...} as possible. It is encouraged but not
necessary to have the rows be of the same type.
"""
rows(cursor::DatabaseCursor{T}) where {T<:DatabaseInterface} = throw(NotImplementedError{T}())

"""
Create a column iterator.

This method should return an instance of an iterator type which returns one
column on each iteration. Each column should be returned as a Vector{...} with
as much type information in the Vector{...} as possible.

This method is optional if rows can have different lengths or sets of values.
"""
columns(cursor::DatabaseCursor{T}) where {T<:DatabaseInterface} = throw(NotSupportedError{T}())

"""
Get result value from a database cursor.

This method gets a single result value in row `i` in column `j`.

This method is optional if rows or columns do not have a defined order.
"""
function Base.getindex(cursor::FixedLengthDatabaseCursor{T},
                       i::Integer, j::Integer) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row `i` in column named `col`.

This method is optional if rows do not have a defined order or if columns do
not have names.
"""
function Base.getindex(cursor::FixedLengthDatabaseCursor{T},
                       i::Integer, col::Symbol) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row named `row` in column `j`.

This method is optional if rows do not have names/keys or if columns do not
have a defined order.
"""
function Base.getindex(cursor::FixedLengthDatabaseCursor{T},
                       row::Symbol, j::Integer) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row named `row` in column named `col`.

This method is optional if rows do not have names/keys or if columns do not
have names.
"""
function Base.getindex(cursor::FixedLengthDatabaseCursor{T},
                       row::Symbol, col::Symbol) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get result value from a database cursor.

This method gets a single result value in row indexed by `row` in column
indexed by `col`.

Any other row or column index types are optional.
"""
function Base.getindex(cursor::FixedLengthDatabaseCursor{T},
                       row::Any, col::Any) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get result value from a database cursor.

Indexing is not required for types which don't subtype
FixedLengthDatabaseCursor.
"""
function Base.getindex(cursor::DatabaseCursor{T}, row::Any, col::Any) where {T<:DatabaseInterface}
    throw(NotSupportedError{T}())
end

"""
Get the number of rows available from a database cursor.

Returns `Int`
"""
function Base.length(cursor::FixedLengthDatabaseCursor{T}) where {T<:DatabaseInterface}
    throw(NotImplementedError{T}())
end

"""
Get the number of rows available from a database cursor.

`length` is not required for types which don't subtype
FixedLengthDatabaseCursor.
"""
Base.length(cursor::DatabaseCursor{T}) where {T<:DatabaseInterface} = throw(NotSupportedError{T}())

"""
A terrible hack to make the fetchintoarray! signature work.

See https://github.com/JuliaLang/julia/issues/13156#issuecomment-140618981
"""
const RevAbstractDict{V, K} = AbstractDict{K,V}

index_return_type(a::AbstractDict) = valtype(a)
index_return_type(a::Any) = eltype(a)

each_index_tuple(a::AbstractDict) = eachindex(a)
each_index_tuple(a::Any) = map(ind -> Tuple(CartesianIndices(A))[ind], eachindex(a))

"""
Get results from a database cursor and store them in a preallocated
two-dimensional data structure.

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `AbstractArray` abstract types. It uses the `getindex` functions
defined above.

When 2d indexing is used on an `Associative`, the result is usually tuple keys.

Returns the preallocated data structure.
"""
function fetchintoarray!(preallocated::Union{AbstractArray, AbstractDict},
                         cursor::FixedLengthDatabaseCursor{T},
                         offset::Int=0) where {T<:DatabaseInterface}
    offset_row = offset

    for (row, column) in each_index_tuple(preallocated)
        offset_row = row + offset
        preallocated[row, column] = cursor[offset_row, column]
    end

    return preallocated, offset_row
end

"""
Get results from a database cursor and store them in a preallocated vector.

This out-of-the-box method supports a huge variety of data structures under the
`AbstractVector` supertype. It uses the `getindex` functions defined above.

Returns the preallocated vector.
"""
function fetchintoarray!(preallocated::AbstractVector, cursor::FixedLengthDatabaseCursor{T},
                         offset::Int=0) where {T<:DatabaseInterface}
    offset_row = offset

    for row in eachindex(preallocated)
        offset_row = row + offset
        preallocated[row] = cursor[offset_row, 1]
    end

    return preallocated, offset_row
end

"""
Get results from a database cursor and store them in a preallocated data
structure (a collection of rows).

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `AbstractDict` supertypes. It uses the `getindex` functions
defined above.

Returns the preallocated data structure.
"""
function fetchintorows!(preallocated::Union{AbstractArray{U}, RevAbstractDict{U}},
                        cursor::FixedLengthDatabaseCursor{T},
                        offset::Int=0) where {T<:DatabaseInterface,U<:Union{AbstractArray,AbstractDict}}
    offset_row = offset

    for row in eachindex(preallocated)
        offset_row = row + offset
        for column in eachindex(preallocated[row])
            preallocated[row][column] = cursor[offset_row, column]
        end
    end

    return preallocated, offset_row
end

"""
Get results from a database cursor and store them in a preallocated data
structure (a collection of columns).

This out-of-the-box method supports a huge variety of data structures under the
`AbstractArray` and `Associative` supertypes. It uses the `getindex` functions
defined above.

`offset` represents the offset into the cursor denoting where to start fetching
data.

Returns the preallocated data structure.
"""
function fetchintocolumns!(preallocated::Union{AbstractArray{U}, AssociativeVK{U}},
                           cursor::FixedLengthDatabaseCursor{T},
                           offset::Int=0) where {T<:DatabaseInterface,
                                                 U<:Union{AbstractArray,AbstractDict}}
    offset_row = offset

    for column in eachindex(preallocated), row in eachindex(preallocated[column])
        offset_row = row + offset
        preallocated[column][row] = cursor[offset_row, column]
    end

    return preallocated, offset_row
end

function fetchintoarray!(preallocated::Union{AbstractArray, AbstractDict},
                         cursor::DatabaseCursor{T}, offset::Int=0) where {T<:DatabaseInterface}
    throw(NotSupportedError{T}())
end

function fetchintorows!(preallocated::Union{AbstractArray{U}, RevAbstractDict{U}},
                        cursor::DatabaseCursor{T},
                        offset::Int=0) where {T<:DatabaseInterface,U<:Union{AbstractArray,AbstractDict}}
    throw(NotSupportedError{T}())
end

function fetchintocolumns!(preallocated::Union{AbstractArray{U}, RevAbstractDict{U}},
                           cursor::DatabaseCursor{T},
                           offset::Int=0) where {T<:DatabaseInterface,
                                                 U<:Union{AbstractArray,AbstractDict}}
    throw(NotSupportedError{T}())
end

abstract type Orientation end
struct RowOriented <: Orientation end
struct ColumnOriented <: Orientation end
struct ArrayOriented <: Orientation end

struct DatabaseFetcher{O<:Orientation, T<:DatabaseInterface, U<:Union{AbstractArray, AbstractDict}}
    preallocated::U
    cursor::FixedLengthDatabaseCursor{T}
end

fetch_function(::DatabaseFetcher{ColumnOriented}) = fetchintocolumns!
fetch_function(::DatabaseFetcher{RowOriented}) = fetchintorows!
fetch_function(::DatabaseFetcher{ArrayOriented}) = fetchintoarray!

first_empty(a::AbstractDict) = isempty(first(values(a)))
first_empty(a) = isempty(first(a))

function isempty(fetcher::DatabaseFetcher{O}) where {O<:Union{RowOriented,ArrayOriented}}
    isempty(fetcher.preallocated)
end

function isempty(fetcher::DatabaseFetcher{O, T, U}) where {O<:ColumnOriented}
    isempty(fetcher.preallocated) || first_empty(fetcher.preallocated)
end

Base.iterate(fetcher::DatabaseFetcher) = (fetch.preallocated, (fetch.preallocated, 0))

function Base.iterate(fetcher::DatabaseFetcher, state)
    preallocated, offset = state
    (offset ≥ length(fetcher.cursor) || isempty(fetcher)) && (return nothing)
    preallocated, new_offset = fetch_function(fetcher)(preallocated, fetcher.cursor, offset)
    preallocated, (preallocated, new_offset)
end

end # module
