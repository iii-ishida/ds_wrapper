defmodule DsWrapper.Value do
  @moduledoc """
  A wrapper for GoogleApi.Datastore.V1.Model.Value
  """

  alias GoogleApi.Datastore.V1.Model.{ArrayValue, Entity, Key, LatLng, Value}

  @doc """
  convert to a `GoogleApi.Datastore.V1.Model.Value`

  ## Examples

      iex> DsWrapper.Value.from_native(123)
      %GoogleApi.Datastore.V1.Model.Value{integerValue: "123"}

      iex> DsWrapper.Value.from_native(123, true)
      %GoogleApi.Datastore.V1.Model.Value{integerValue: "123", excludeFromIndexes: true}
  """
  def from_native(value, exclude_from_index \\ nil)

  def from_native(value, exclude_from_index) when is_integer(value) do
    %Value{integerValue: Integer.to_string(value), excludeFromIndexes: exclude_from_index}
  end

  def from_native(value, exclude_from_index) when is_float(value) do
    %Value{doubleValue: value, excludeFromIndexes: exclude_from_index}
  end

  def from_native(value, exclude_from_index) when is_bitstring(value) do
    %Value{stringValue: value, excludeFromIndexes: exclude_from_index}
  end

  def from_native(value, exclude_from_index) when is_boolean(value) do
    %Value{booleanValue: value, excludeFromIndexes: exclude_from_index}
  end

  def from_native(value, exclude_from_index) when is_nil(value) do
    %Value{nullValue: "NULL_VALUE", excludeFromIndexes: exclude_from_index}
  end

  def from_native(value, exclude_from_index) when is_list(value) do
    values = Enum.map(value, &from_native/1)
    %Value{arrayValue: %ArrayValue{values: values}, excludeFromIndexes: exclude_from_index}
  end

  def from_native(%{latitude: lat, longitude: lon}, exclude_from_index) do
    %Value{geoPointValue: %LatLng{latitude: lat, longitude: lon}, excludeFromIndexes: exclude_from_index}
  end

  def from_native(%Key{} = value, exclude_from_index) do
    %Value{keyValue: value, excludeFromIndexes: exclude_from_index}
  end

  def from_native(%Entity{} = value, exclude_from_index) do
    %Value{entityValue: value, excludeFromIndexes: exclude_from_index}
  end

  def from_native(%DateTime{} = value, exclude_from_index) do
    %Value{timestampValue: value, excludeFromIndexes: exclude_from_index}
  end

  @doc """
  convert from a `GoogleApi.Datastore.V1.Model.Value`

  ## Examples

      iex> DsWrapper.Value.to_native(%GoogleApi.Datastore.V1.Model.Value{integerValue: "123"})
      123
  """
  def to_native(%Value{integerValue: value}) when not is_nil(value) do
    value |> Integer.parse() |> elem(0)
  end

  def to_native(%Value{arrayValue: value}) when not is_nil(value) do
    Enum.map(value.values, &to_native/1)
  end

  def to_native(%Value{entityValue: value}) when not is_nil(value) do
    Enum.reduce(value.properties, %{}, fn {key, value}, acc ->
      Map.merge(acc, %{key => to_native(value)})
    end)
  end

  def to_native(%Value{geoPointValue: %LatLng{latitude: lat, longitude: lon}} = value) when not is_nil(value) do
    %{latitude: lat, longitude: lon}
  end

  def to_native(%Value{} = value) do
    not_nil_value = Map.from_struct(value)
                    |> Enum.find(fn {k, v} -> k != :excludeFromIndexes && v != nil end)

    case not_nil_value do
      {_, v} -> v
      _ -> nil
    end
  end
end
