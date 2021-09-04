defmodule DsWrapper.Connection do
  @moduledoc """
  Connection for GoogleApi.Datastore.V1.
  """

  alias GoogleApi.Datastore.V1.Connection

  defstruct [:connection, :project_id, :transaction_id, :mutation_store_pid]

  @type t :: %__MODULE__{
          connection: Tesla.Client.t(),
          project_id: String.t(),
          transaction_id: String.t() | nil,
          mutation_store_pid: pid | nil
        }

  @token Application.compile_env(:ds_wrapper, :token_for_connection, Goth.Token)

  @doc """
  Configure a client connection.
  """
  def new(token, project_id) do
    %__MODULE__{connection: Connection.new(token), project_id: project_id}
  end

  @doc """
  Configure a client connection.
  """
  def new(project_id) do
    case @token.for_scope("https://www.googleapis.com/auth/datastore") do
      {:ok, token} -> {:ok, new(token.token, project_id)}
      error -> error
    end
  end

  @doc """
  Configure a client connection.
  use GOOGLE_CLOUD_PROJECT as the project_id.
  """
  def new, do: new(System.get_env("GOOGLE_CLOUD_PROJECT"))
end

defmodule DsWrapper.Token do
  @moduledoc false

  @callback for_scope(String.t()) :: {:ok, term()} | {:error, term()}
end
