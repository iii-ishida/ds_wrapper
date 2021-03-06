defmodule DsWrapper.Query do
  @moduledoc """
  `GoogleApi.Datastore.V1.Model.Query` wrapper
  """

  alias DsWrapper.Value

  alias GoogleApi.Datastore.V1.Model.{
    CompositeFilter,
    Filter,
    KindExpression,
    Projection,
    PropertyFilter,
    PropertyOrder,
    PropertyReference,
    Query
  }

  @type query :: %Query{}

  @doc """
  Create a new `%GoogleApi.Datastore.V1.Model.Query` for kind.

  ## Examples

      iex> DsWrapper.Query.new_query("SomeKind")
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec new_query(String.t()) :: %Query{}
  def new_query(kind) do
    %Query{kind: %KindExpression{name: kind}}
  end

  @doc """
  Add a property filter to the query.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> where("some_property", "=", "some value")
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec where(query, String.t(), String.t(), term) :: query
  def where(%Query{} = query, property, operator, value) do
    current_composite_filter =
      case query.filter do
        %{compositeFilter: composite_filter} -> composite_filter
        _ -> %CompositeFilter{filters: [], op: "AND"}
      end

    new_filter = %Filter{
      propertyFilter: %PropertyFilter{
        property: new_property_reference(property),
        op: to_op(operator),
        value: Value.from_native(value)
      }
    }

    new_filters = current_composite_filter.filters ++ [new_filter]
    new_composite_filter = %CompositeFilter{current_composite_filter | filters: new_filters}

    %Query{query | filter: %Filter{compositeFilter: new_composite_filter}}
  end

  @doc """
  Retrieve only select properties from the matched entities.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> select(["some_property", ...])
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec select(query, list(String.t())) :: query
  def select(%Query{} = query, property_names) do
    projection_properties = Enum.map(property_names, &%Projection{property: %PropertyReference{name: &1}})
    %Query{query | projection: projection_properties}
  end

  @doc """
  Group results by a list of properties.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> group_by(["some_property", ...])
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec group_by(query, list(String.t())) :: query
  def group_by(%Query{} = query, property_names) do
    distinct_properties = Enum.map(property_names, &%PropertyReference{name: &1})
    %Query{query | distinctOn: distinct_properties}
  end

  @doc """
  Sort the results by a property name. By default, an ascending sort order will be used.
  To sort in descending order, provide a second argument to `:desc`.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> order("some_property")
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec order(query, String.t(), :asc | :desc | nil) :: query
  def order(%Query{} = query, property, direction \\ nil) do
    order = [new_property_order(property, to_direction(direction))]

    %Query{query | order: (query.order || []) ++ order}
  end

  @doc """
  Set a limit on the number of results to be returned.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> limit(100)
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec limit(query, non_neg_integer) :: query
  def limit(%Query{} = query, limit) do
    %Query{query | limit: limit}
  end

  @doc """
  Set the cursor to start the results at.

  ## Examples

      iex> import DsWrapper.Query
      iex> new_query("SomeKind")
      ...> |> start(cursor)
      %GoogleApi.Datastore.V1.Model.Query{...}
  """
  @spec start(query, String.t()) :: query
  def start(%Query{} = query, cursor) do
    %Query{query | startCursor: cursor}
  end

  defp new_property_order(property, direction) do
    %PropertyOrder{
      property: new_property_reference(property),
      direction: direction
    }
  end

  defp new_property_reference(property) do
    %PropertyReference{name: property}
  end

  defp to_op("="), do: "EQUAL"
  defp to_op("<"), do: "LESS_THAN"
  defp to_op("<="), do: "LESS_THAN_OR_EQUAL"
  defp to_op(">"), do: "GREATER_THAN"
  defp to_op(">="), do: "GREATER_THAN_OR_EQUAL"

  defp to_direction(:desc), do: "DESCENDING"
  defp to_direction(_), do: "ASCENDING"
end
