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

      iex> DsWrapper.Key.new("SomeKind", 1234)
      %GoogleApi.Datastore.V1.Model.Key{path: [%GoogleApi.Datastore.V1.Model.PathElement{kind: "SomeKind", id: "1234"}]}
  """
  def new(kind, id_or_name \\ nil, parent \\ %Key{path: []})

  def new(kind, id, parent) when is_integer(id) do
    %Key{path: parent.path ++ [%PathElement{kind: kind, id: Integer.to_string(id)}]}
  end

  def new(kind, name, parent) do
    %Key{path: parent.path ++ [%PathElement{kind: kind, name: name}]}
  end
end
