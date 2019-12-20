defmodule DsWrapper.Datastore do
  @moduledoc """
  utility for GoogleApi.Datastore.V1.Api.Projects
  """

  alias GoogleApi.Datastore.V1.Model.{
    LookupRequest,
    Query,
    ReadOptions,
    RunQueryRequest
  }

  @google_api_projects Application.get_env(:ds_wrapper, :google_api_projects, GoogleApi.Datastore.V1.Api.Projects)

  defdelegate query(kind), to: DsWrapper.Query, as: :new_query

  @doc """
  retrieve entities specified by a Query.

  ## Examples

      iex> import DsWrapper.Query
      ...> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> query = new_query("SomeKind") |> where("some_property", "=", "some value")
      ...> DsWrapper.Datastore.run_query(connection, query)
      {:ok, %{cursor: ..., entities: [%{...}]}}
  """
  def run_query(connection, %Query{} = query) do
    req = %RunQueryRequest{query: query, readOptions: %ReadOptions{}}

    with {:ok, result} <- call_datastore_api(connection, &@google_api_projects.datastore_projects_run_query/3, body: req) do
      cursor = result.batch.endCursor
      entity_results = result.batch.entityResults || []
      entities = Enum.map(entity_results, &DsWrapper.Entity.to_map/1)
      {:ok, %{cursor: cursor, entities: entities}}
    end
  end

  @doc """
  retrieve an entity by key.

  ## Examples

      iex> import DsWrapper.Query
      ...> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> key = DsWrapper.Key.new("SomeKind", "some-name")
      ...> DsWrapper.Datastore.find(connection, key)
      {:ok, %{...}}
  """
  def find(connection, key) do
    with {:ok, %{found: found}} <- lookup(connection, [key]) do
      entity =
        (found || [])
        |> List.first()
        |> DsWrapper.Entity.to_map()

      {:ok, entity}
    end
  end

  @doc """
  retrieve the entities for the provided keys. The order of results is undefined and has no relation to the order of keys arguments.

  ## Examples

      iex> import DsWrapper.Query
      ...> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> keys = [DsWrapper.Key.new("SomeKind", "some-name-01"), ...]
      ...> DsWrapper.Datastore.find_all(connection, keys)
      {:ok, %{found: [%{...}, ...], missing: [%Key{...}, ...], deferred: [%Key{...}, ...]}}
  """
  def find_all(connection, keys) do
    with {:ok, result} <- lookup(connection, keys) do
      {:ok,
       %{
         found: result.found && Enum.map(result.found, &DsWrapper.Entity.to_map/1),
         missing: result.missing && Enum.map(result.missing, & &1.entity.key),
         deferred: result.deferred
       }}
    end
  end

  defp lookup(connection, keys) do
    req = %LookupRequest{
      keys: keys,
      readOptions: %ReadOptions{}
    }

    call_datastore_api(connection, &@google_api_projects.datastore_projects_lookup/3, body: req)
  end

  defp call_datastore_api(%{connection: conn, project_id: project_id}, fun, optional_params) do
    case fun.(conn, project_id, optional_params) do
      {:ok, _} = result -> result
      {:error, _} = error -> error
      reason -> {:error, reason}
    end
  end
end

defmodule DsWrapper.GoogleApiProjects do
  @moduledoc false

  @callback datastore_projects_run_query(Tesla.Env.client(), String.t(), keyword) :: {:ok, GoogleApi.Datastore.V1.Model.RunQueryResponse.t()} | {:error, Tesla.Env.t()}
  @callback datastore_projects_lookup(Tesla.Env.client(), String.t(), keyword) :: {:ok, GoogleApi.Datastore.V1.Model.LookupResponse.t()} | {:error, Tesla.Env.t()}
end
