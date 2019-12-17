defmodule DsWrapper.Connection do
  @moduledoc """
  a connection for GoogleApi.Datastore.V1.
  """

  alias GoogleApi.Datastore.V1.Connection

  defstruct [:connection, :project_id]

  @token Application.get_env(:ds_wrapper, :token_for_connection, Goth.Token)

  @doc """
  configure a client connection.
  """
  def new(project_id) do
    case @token.for_scope("https://www.googleapis.com/auth/datastore") do
      {:ok, token} -> {:ok, %__MODULE__{connection: Connection.new(token.token), project_id: project_id}}
      error -> error
    end
  end
end

defmodule DsWrapper.Token do
  @moduledoc false

  @callback for_scope(String.t()) :: {:ok, term()} | {:error, term()}
end
