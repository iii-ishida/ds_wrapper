use Mix.Config

config :goth, disabled: true

config :ds_wrapper, :token_for_connection, DsWrapper.TokenMock
