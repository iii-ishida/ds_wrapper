defmodule DsWrapper.Datastore do
  @moduledoc """
  utility for GoogleApi.Datastore.V1.Api.Projects
  """

  alias GoogleApi.Datastore.V1.Model.{
    CommitRequest,
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

  @doc """
  insert one or more entities to the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> entity = DsWrapper.Entity.new(key, properties)
      ...> DsWrapper.Datastore.insert(connection, entity)
      {:ok, [%Key{...}]}
  """
  def insert(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_insert/1, entities)

  @doc """
  persist one or more entities to the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> entity = DsWrapper.Entity.new(key, properties)
      ...> DsWrapper.Datastore.upsert(connection, entity)
      {:ok, [%Key{...}]}
  """
  def upsert(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_upsert/1, entities)

  @doc """
  update one or more entities to the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> entity = DsWrapper.Entity.new(key, properties)
      ...> DsWrapper.Datastore.update(connection, entity)
      {:ok, [%Key{...}]}
  """
  def update(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_update/1, entities)

  @doc """
  remove entities from the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> key = DsWrapper.Key.new("SomeKind", "some-name")
      ...> DsWrapper.Datastore.delete(connection, key)
      :ok
  """
  def delete(connection, keys) do
    with {:ok, _} <- do_command(connection, &DsWrapper.Mutation.for_delete/1, keys) do
      :ok
    end
  end

  def commit(connection, mutations) do
    req = %CommitRequest{
      mode: "NON_TRANSACTIONAL",
      mutations: mutations
    }

    call_datastore_api(connection, &@google_api_projects.datastore_projects_commit/3, body: req)
  end

  defp do_command(connection, create_mutations_function, entities_or_keys) when is_list(entities_or_keys) do
    with {:ok, response} <- commit(connection, create_mutations_function.(entities_or_keys)) do
      {:ok, keys_from_commit_response(response, entities_or_keys)}
    end
  end

  defp do_command(connection, create_mutations_function, entity_or_key) do
    do_command(connection, create_mutations_function, [entity_or_key])
  end

  defp keys_from_commit_response(%{mutationResults: mutation_results}, entities_for_default) do
    mutation_results
    |> Enum.with_index()
    |> Enum.map(fn {result, i} -> result.key || Enum.at(entities_for_default, i) |> Map.get(:key) end)
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
  @callback datastore_projects_commit(Tesla.Env.client(), String.t(), keyword) :: {:ok, GoogleApi.Datastore.V1.Model.CommitResponse.t()} | {:error, Tesla.Env.t()}
end
