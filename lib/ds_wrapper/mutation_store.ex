defmodule DsWrapper.MutationStore do
  @moduledoc false

  use Agent

  def start_link do
    Agent.start_link(fn -> [] end)
  end

  def put(pid, mutations) do
    Agent.update(pid, &(&1 ++ mutations))
  end

  def get_all(pid) do
    Agent.get(pid, & &1)
  end

  def stop(pid) do
    if Process.alive?(pid) do
      Agent.stop(pid)
    end
  end
end
