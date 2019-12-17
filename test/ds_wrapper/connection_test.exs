defmodule DsWrapper.ConnectionTest do
  use ExUnit.Case, async: true
  import Mox

  alias DsWrapper.TokenMock

  test "new/1" do
    TokenMock
    |> expect(:for_scope, fn scope ->
      assert scope == "https://www.googleapis.com/auth/datastore"

      {:ok, %Goth.Token{token: <<1, 2, 3>>}}
    end)

    assert {:ok, %DsWrapper.Connection{} = connection} = DsWrapper.Connection.new("test-project")
    assert %Tesla.Client{} = connection.connection
    assert connection.project_id == "test-project"
  end
end
