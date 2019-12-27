defmodule DsWrapper.Mutation do
  @moduledoc """
  `GoogleApi.Datastore.V1.Model.Mutation` wrapper
  """

  alias GoogleApi.Datastore.V1.Model.Mutation

  @doc """
  Create new `GoogleApi.Datastore.V1.Model.Mutation`s for insert.

  ## Examples
      iex> DsWrapper.Mutation.for_insert(DsWrapper.Entity.new(...))
      [%GoogleApi.Datastore.V1.Model.Mutation{insert: ...}]
  """
  def for_insert(entities) when is_list(entities) do
    Enum.map(entities, &%Mutation{insert: &1})
  end

  def for_insert(entity) do
    for_insert([entity])
  end

  @doc """
  Create new `GoogleApi.Datastore.V1.Model.Mutation`s for upsert.

  ## Examples
      iex> DsWrapper.Mutation.for_upsert(DsWrapper.Entity.new(...))
      [%GoogleApi.Datastore.V1.Model.Mutation{upsert: ...}]
  """
  def for_upsert(entities) when is_list(entities) do
    Enum.map(entities, &%Mutation{upsert: &1})
  end

  def for_upsert(entity) do
    for_upsert([entity])
  end

  @doc """
  Create new `GoogleApi.Datastore.V1.Model.Mutation`s for update.

  ## Examples
      iex> DsWrapper.Mutation.for_update(DsWrapper.Entity.new(...))
      [%GoogleApi.Datastore.V1.Model.Mutation{update: ...}]
  """
  def for_update(entities) when is_list(entities) do
    Enum.map(entities, &%Mutation{update: &1})
  end

  def for_update(entity) do
    for_update([entity])
  end

  @doc """
  Create new `GoogleApi.Datastore.V1.Model.Mutation`s for delete.

  ## Examples
      iex> DsWrapper.Mutation.for_delete(DsWrapper.Key.new(...))
      [%GoogleApi.Datastore.V1.Model.Mutation{delete: ...}]
  """
  def for_delete(keys) when is_list(keys) do
    Enum.map(keys, &%Mutation{delete: &1})
  end

  def for_delete(key) do
    for_delete([key])
  end
end
