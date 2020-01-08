defmodule DsWrapper.MixProject do
  use Mix.Project

  @description "google_api_datastore のラッパー"

  def project do
    [
      app: :ds_wrapper,
      version: "0.1.1",
      description: @description,
      elixir: "~> 1.9",
      deps: deps(),
      build_embedded: Mix.env() == :prod,
      package: package(),
      source_url: "https://github.com/iii-ishida/ds_wrapper",
      dialyzer: [plt_file: {:no_warn, "priv/plts/dialyzer.plt"}],
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:google_api_datastore, "~> 0.12"},
      {:goth, "~> 1.1.0"},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.1.0", only: [:dev, :test], runtime: false},
      {:earmark, "~> 1.4", only: :dev},
      {:ex_doc, "~> 0.21", only: :dev},
      {:mox, "~> 0.5", only: :test}
    ]
  end

  defp elixirc_paths(:test), do: ["test/support", "lib"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    %{
      files: ["lib", "mix.exs", "README*", "LICENSE"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/iii-ishida/ds_wrapper"}
    }
  end
end
