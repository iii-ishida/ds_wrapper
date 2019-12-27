defmodule DsWrapper.Entity do
  @moduledoc """
  `GoogleApi.Datastore.V1.Model.Entity` wrapper
  """

  alias DsWrapper.Value
  alias GoogleApi.Datastore.V1.Model.{Entity, EntityResult, Key}

  @doc """
  Create a new `GoogleApi.Datastore.V1.Model.Entity`.

  ## Examples

      iex> key = DsWrapper.Key.new("SomeKind", "some-name")
      iex> properties = %{"some_property" => "some value"}
      iex> DsWrapper.Entity.new(key, properties)
      %GoogleApi.Datastore.V1.Model.Entity{...}
  """
  @spec new(%Key{}, %{required(String.t()) => term}, list(String.t())) :: Entity.t()
  def new(key, properties, exclude_from_indexes \\ []) do
    %Entity{
      key: key,
      properties:
        Enum.reduce(properties, %{}, fn {key, value}, acc ->
          exclude_from_index = Enum.member?(exclude_from_indexes, key)
          Map.merge(acc, %{key => Value.from_native(value, exclude_from_index)})
        end)
    }
  end

  @doc """
  Convert `GoogleApi.Datastore.V1.Model.Entity`'s properties to a Map.

  ## Examples

      iex> DsWrapper.Entity.to_map(entity)
      %{"some_property" => "some value"}
  """
  @spec to_map(nil | EntityResult.t() | Entity.t()) :: %{required(String.t()) => term}
  def to_map(nil), do: nil
  def to_map(%EntityResult{entity: entity}), do: to_map(entity)

  def to_map(%Entity{properties: properties}) do
    Enum.reduce(properties, %{}, fn {key, value}, acc ->
      Map.merge(acc, %{key => Value.to_native(value)})
    end)
  end
end
