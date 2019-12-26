defmodule DsWrapper.Datastore do
  @moduledoc """
  utility for GoogleApi.Datastore.V1.Api.Projects
  """

  alias GoogleApi.Datastore.V1.Model.{
    BeginTransactionRequest,
    CommitRequest,
    CommitResponse,
    Entity,
    Key,
    LookupRequest,
    Query,
    ReadOnly,
    ReadOptions,
    ReadWrite,
    RollbackRequest,
    RunQueryRequest,
    TransactionOptions
  }

  @type key :: %Key{}
  @type query_result :: %{cursor: String.t() | nil, entities: list(map)}
  @type find_all_result :: %{
          found: list(map) | nil,
          missing: list(key) | nil,
          deferred: list(key) | nil
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
  @spec run_query(DsWrapper.Connection.t(), Query.t()) :: {:ok, query_result} | {:error, term}
  def run_query(connection, %Query{} = query) do
    req = %RunQueryRequest{query: query, readOptions: %ReadOptions{transaction: connection.transaction_id}}

    with {:ok, result} <- call_datastore_api(connection, &@google_api_projects.datastore_projects_run_query/3, body: req) do
      cursor = result.batch.endCursor
      entity_results = result.batch.entityResults || []
      entities = Enum.map(entity_results, &DsWrapper.Entity.to_map/1)
      {:ok, %{cursor: cursor, entities: entities}}
    end
  end

  @doc """
  retrieve entities specified by a Query. Raises an exception on error.
  """
  @spec run_query!(DsWrapper.Connection.t(), Query.t()) :: query_result | no_return
  def run_query!(connection, %Query{} = query) do
    case run_query(connection, query) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
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
  @spec find(DsWrapper.Connection.t(), key) :: {:ok, map | nil} | {:error, term}
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
  retrieve an entity by key. Raises an exception on error.
  """
  @spec find!(DsWrapper.Connection.t(), key) :: map | nil | no_return
  def find!(connection, key) do
    case find(connection, key) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
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
  @spec find_all(DsWrapper.Connection.t(), list(key)) :: {:ok, find_all_result} | {:error, term}
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
  retrieve the entities for the provided keys. The order of results is undefined and has no relation to the order of keys arguments.
  Raises an exception on error.
  """
  @spec find_all!(DsWrapper.Connection.t(), list(key)) :: find_all_result | no_return
  def find_all!(connection, keys) do
    case find_all(connection, keys) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
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
  @spec insert(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: {:ok, [key] | nil} | {:error, term}
  def insert(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_insert/1, entities)

  @doc """
  insert one or more entities to the Datastore. Raises an exception on error.
  """
  @spec insert!(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: [key] | nil | no_return
  def insert!(connection, entities), do: do_command!(connection, &DsWrapper.Mutation.for_insert/1, entities)

  @doc """
  persist one or more entities to the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> entity = DsWrapper.Entity.new(key, properties)
      ...> DsWrapper.Datastore.upsert(connection, entity)
      {:ok, [%Key{...}]}
  """
  @spec upsert(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: {:ok, [key] | nil} | {:error, term}
  def upsert(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_upsert/1, entities)

  @doc """
  persist one or more entities to the Datastore. Raises an exception on error.
  """
  @spec upsert!(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: [key] | nil | no_return
  def upsert!(connection, entities), do: do_command!(connection, &DsWrapper.Mutation.for_upsert/1, entities)

  @doc """
  update one or more entities to the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> entity = DsWrapper.Entity.new(key, properties)
      ...> DsWrapper.Datastore.update(connection, entity)
      {:ok, [%Key{...}]}
  """
  @spec update(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: {:ok, [key] | nil} | {:error, term}
  def update(connection, entities), do: do_command(connection, &DsWrapper.Mutation.for_update/1, entities)

  @doc """
  update one or more entities to the Datastore. Raises an exception on error.
  """
  @spec update!(DsWrapper.Connection.t(), list(Entity.t()) | Entity.t()) :: [key] | nil | no_return
  def update!(connection, entities), do: do_command!(connection, &DsWrapper.Mutation.for_update/1, entities)

  @doc """
  remove entities from the Datastore.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> key = DsWrapper.Key.new("SomeKind", "some-name")
      ...> DsWrapper.Datastore.delete(connection, key)
      :ok
  """
  @spec delete(DsWrapper.Connection.t(), list(key) | key) :: :ok | {:error, term}
  def delete(connection, keys) do
    with {:ok, _} <- do_command(connection, &DsWrapper.Mutation.for_delete/1, keys) do
      :ok
    end
  end

  @doc """
  remove entities from the Datastore. Raises an exception on error.
  """
  @spec delete!(DsWrapper.Connection.t(), list(key) | key) :: :ok | no_return
  def delete!(connection, keys) do
    with {:error, reason} <- delete(connection, keys) do
      raise reason
    end
  end

  @doc """
  creates a Datastore Transaction.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> DsWrapper.transaction(conn)
      {:ok, %DsWrapper.Connection{connection: ..., project_id: ..., transaction_id: ..., mutation_store_pid: ...}}
  """
  @spec transaction(DsWrapper.Connection.t(), atom) :: {:ok, DsWrapper.Connection.t()} | {:error, term}
  def transaction(connection, read_only \\ nil) do
    with {:ok, %{transaction: tx_id}} <- begin_transaction(connection, read_only == :read_only),
         {:ok, pid} <- DsWrapper.MutationStore.start_link() do
      {:ok, %DsWrapper.Connection{connection | transaction_id: tx_id, mutation_store_pid: pid}}
    end
  end

  @doc """
  Runs the given function inside a transaction.
  if the given function raises exception, rolled back the transaction.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> DsWrapper.run_in_transaction(conn, fn tx ->
      ...>   Datastore.insert!(tx, entity)
      ...>   Datastore.update!(tx, another_entity)
      ...> end)
      {:ok, ...}
  """
  @spec run_in_transaction(DsWrapper.Connection.t(), (DsWrapper.Connection.t() -> term)) :: {:ok, term} | {:error, term}
  def run_in_transaction(connection, fun) do
    with {:ok, tx_conn} <- transaction(connection) do
      try do
        result = fun.(tx_conn)
        commit(tx_conn)
        {:ok, result}
      rescue
        e ->
          rollback(tx_conn)
          {:error, e}
      end
    end
  end

  @doc """
  commit a transaction.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> {:ok, tx} = DsWrapper.transaction(conn)
      ...> DsWrapper.Datastore.insert(tx, entity_a)
      ...> DsWrapper.Datastore.update(tx, entity_b)
      ...> DsWrapper.Datastore.commit(tx)
      {:ok, %CommitResponse{...}}
  """
  @spec commit(DsWrapper.Connection.t()) :: {:ok, CommitResponse.t()} | {:error, term}
  def commit(tx_connection) do
    req = %CommitRequest{
      mode: "TRANSACTIONAL",
      mutations: DsWrapper.MutationStore.get_all(tx_connection.mutation_store_pid),
      transaction: tx_connection.transaction_id
    }

    DsWrapper.MutationStore.stop(tx_connection.mutation_store_pid)

    call_datastore_api(tx_connection, &@google_api_projects.datastore_projects_commit/3, body: req)
  end

  @doc """
  rolls a transaction back.

  ## Examples

      iex> {:ok, connection} = DsWrapper.Connection.new("project-id")
      ...> {:ok, tx} = DsWrapper.transaction(conn)
      ...> DsWrapper.Datastore.insert(tx, entity_a)
      ...> DsWrapper.Datastore.update(tx, entity_b)
      ...> DsWrapper.Datastore.rollback(tx)
      :ok
  """
  @spec rollback(DsWrapper.Connection.t()) :: :ok | {:error, term}
  def rollback(tx_connection) do
    req = %RollbackRequest{transaction: tx_connection.transaction_id}

    DsWrapper.MutationStore.stop(tx_connection.mutation_store_pid)

    with {:ok, _} <- call_datastore_api(tx_connection, &@google_api_projects.datastore_projects_rollback/3, body: req) do
      :ok
    end
  end

  defp do_command(connection, create_mutations_function, entity_or_key) when not is_list(entity_or_key) do
    do_command(connection, create_mutations_function, [entity_or_key])
  end

  defp do_command(%{transaction_id: tx_id} = connection, create_mutations_function, entities_or_keys) when is_nil(tx_id) do
    with {:ok, response} <- commit(connection, create_mutations_function.(entities_or_keys)) do
      {:ok, keys_from_commit_response(response, entities_or_keys)}
    end
  end

  # for transaction
  defp do_command(%{mutation_store_pid: pid}, create_mutations_function, entities_or_keys) do
    DsWrapper.MutationStore.put(pid, create_mutations_function.(entities_or_keys))
    {:ok, nil}
  end

  defp do_command!(connection, create_mutations_function, entity_or_key) do
    case do_command(connection, create_mutations_function, entity_or_key) do
      {:ok, result} -> result
      {:error, reason} -> raise reason
    end
  end

  defp commit(%{transaction_id: tx_id} = connection = connection, mutations) when is_nil(tx_id) do
    req = %CommitRequest{
      mode: "NON_TRANSACTIONAL",
      mutations: mutations
    }

    call_datastore_api(connection, &@google_api_projects.datastore_projects_commit/3, body: req)
  end

  defp keys_from_commit_response(%{mutationResults: mutation_results}, entities_for_default) do
    mutation_results
    |> Enum.with_index()
    |> Enum.map(fn {result, i} -> result.key || Enum.at(entities_for_default, i) |> Map.get(:key) end)
  end

  defp begin_transaction(connection, read_only?) do
    options =
      if read_only? do
        %TransactionOptions{readOnly: %ReadOnly{}}
      else
        %TransactionOptions{readWrite: %ReadWrite{}}
      end

    req = %BeginTransactionRequest{
      transactionOptions: options
    }

    call_datastore_api(connection, &@google_api_projects.datastore_projects_begin_transaction/3, body: req)
  end

  defp lookup(connection, keys) do
    req = %LookupRequest{
      keys: keys,
      readOptions: %ReadOptions{transaction: connection.transaction_id}
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
  @callback datastore_projects_begin_transaction(Tesla.Env.client(), String.t(), keyword) :: {:ok, GoogleApi.Datastore.V1.Model.BeginTransactionResponse.t()} | {:error, Tesla.Env.t()}
  @callback datastore_projects_rollback(Tesla.Env.client(), String.t(), keyword) :: {:ok, GoogleApi.Datastore.V1.Model.RollbackResponse.t()} | {:error, Tesla.Env.t()}
end
