defmodule DsWrapper.Key do
  @moduledoc """
  A wrapper for `GoogleApi.Datastore.V1.Model.Key`
  """

  alias GoogleApi.Datastore.V1.Model.{Key, PathElement}

  @doc """
    new `GoogleApi.Datastore.V1.Model.Key`

    ## Examples

        iex> DsWrapper.Key.new("SomeKind")
        %GoogleApi.Datastore.V1.Model.Key{path: [%GoogleApi.Datastore.V1.Model.PathElement{kind: "SomeKind"}]}

        iex> DsWrapper.Key.new("SomeKind", "some-name")
        %GoogleApi.Datastore.V1.Model.Key{path: [%GoogleApi.Datastore.V1.Model.PathElement{kind: "SomeKind", name: "some-name"}]}

  """
  def new(kind, name \\ nil, parent \\ %Key{path: []}) do
    %Key{path: parent.path ++ [%PathElement{kind: kind, name: name}]}
  end
end
