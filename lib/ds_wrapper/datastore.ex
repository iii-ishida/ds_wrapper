defmodule DsWrapper.Datastore do
  @moduledoc """
  utility for GoogleApi.Datastore.V1.Api.Projects
  """

  alias GoogleApi.Datastore.V1.Model.{
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
end
