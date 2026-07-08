OFFCHAIN_SCHEMA = """
PostgreSQL off-chain asset market data schema:

Table: asset
- id BIGSERIAL PRIMARY KEY
- asset_id TEXT UNIQUE NOT NULL
- symbol TEXT NOT NULL
- name TEXT
- policy_id TEXT
- asset_name_hex TEXT
- decimals INTEGER NOT NULL DEFAULT 0
- asset_type TEXT NOT NULL DEFAULT 'token'
- created_at TIMESTAMPTZ NOT NULL DEFAULT now()

Table: asset_market_source
- id BIGSERIAL PRIMARY KEY
- asset_id TEXT NOT NULL REFERENCES asset(asset_id)
- source TEXT NOT NULL
- source_asset_id TEXT NOT NULL
- quote_asset TEXT NOT NULL
- base_asset_symbol TEXT
- source_market_id TEXT NOT NULL
- valid_from TIMESTAMPTZ NOT NULL
- valid_to TIMESTAMPTZ
- bootstrap_from TIMESTAMPTZ NOT NULL
- enabled BOOLEAN NOT NULL DEFAULT true
- metadata JSONB NOT NULL DEFAULT '{}'::jsonb
- created_at TIMESTAMPTZ NOT NULL DEFAULT now()
Unique key: (source, source_market_id)

Table: asset_ohlcv
- id BIGSERIAL PRIMARY KEY
- asset_id TEXT NOT NULL REFERENCES asset(asset_id)
- market_source_id BIGINT NOT NULL REFERENCES asset_market_source(id)
- ts TIMESTAMPTZ NOT NULL
- interval TEXT NOT NULL
- open NUMERIC(38,18) NOT NULL
- high NUMERIC(38,18) NOT NULL
- low NUMERIC(38,18) NOT NULL
- close NUMERIC(38,18) NOT NULL
- volume NUMERIC(38,18) NOT NULL
- source TEXT NOT NULL
- source_asset_id TEXT NOT NULL
- quote_asset TEXT NOT NULL
- created_at TIMESTAMPTZ NOT NULL DEFAULT now()
Unique key: (market_source_id, ts, interval)

Table: asset_indicator
- id BIGSERIAL PRIMARY KEY
- asset_id TEXT NOT NULL REFERENCES asset(asset_id)
- ts TIMESTAMPTZ NOT NULL
- interval TEXT NOT NULL
- ohlcv_source TEXT NOT NULL
- indicator TEXT NOT NULL
- period INTEGER NOT NULL
- value NUMERIC(38,18) NOT NULL
- params JSONB NOT NULL DEFAULT '{}'::jsonb
- source TEXT NOT NULL
- created_at TIMESTAMPTZ NOT NULL DEFAULT now()
Unique key: (asset_id, ts, interval, ohlcv_source, indicator, period, params, source)

Table: asset_relationship
- id BIGSERIAL PRIMARY KEY
- from_asset_id TEXT NOT NULL REFERENCES asset(asset_id)
- to_asset_id TEXT NOT NULL REFERENCES asset(asset_id)
- relationship_type TEXT NOT NULL
- effective_at TIMESTAMPTZ
- metadata JSONB NOT NULL DEFAULT '{}'::jsonb
- created_at TIMESTAMPTZ NOT NULL DEFAULT now()
Unique key: (from_asset_id, to_asset_id, relationship_type)

Table: offchain_governance_proposal
- id BIGSERIAL PRIMARY KEY
- provider TEXT NOT NULL
- source_system TEXT NOT NULL
- source_endpoint TEXT NOT NULL
- source_external_id TEXT NOT NULL
- source_url TEXT NOT NULL
- title TEXT
- abstract TEXT
- motivation TEXT
- rationale TEXT
- proposer_name TEXT
- proposer_url TEXT
- proposer_id TEXT
- lifecycle_status TEXT
- governance_action_type TEXT
- governance_action_tx_id TEXT
- governance_action_index INTEGER
- governance_action_id TEXT
- metadata_url TEXT
- metadata_hash TEXT
- requested_lovelace NUMERIC(38,0)
- raw_content TEXT NOT NULL
- raw_text TEXT NOT NULL
- content_sha256 TEXT NOT NULL
- http_status INTEGER
- content_type TEXT
- first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
- last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
- fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
- enabled BOOLEAN NOT NULL DEFAULT true
Unique key: (provider, source_system, source_endpoint, source_external_id, content_sha256)

Query guidance:
- Use asset_ohlcv for price, OHLCV, candles, market volume, open, high, low, close, returns, volatility, and time-series market queries.
- Join asset when the user names a token by symbol, name, policy_id, or asset_name_hex.
- Join asset_market_source when the query depends on exchange/provider, quote asset, source symbol, market validity, or market-specific uniqueness.
- Use asset_indicator for precomputed indicators such as sma, ema, rsi, bb_middle, bb_upper, bb_lower, macd, macd_signal, and macd_histogram.
- Use asset_relationship for mapped relationships between assets, such as wrapped, bridged, derivative, or related market assets.
- Interval data currently available is 1h.
- Use offchain_governance_proposal only for off-chain governance metadata, proposal discussion content, proposer names, titles, rationale, abstracts, metadata URLs, and public governance-page status labels.
- Do not use offchain_governance_proposal as a substitute for onchain facts already present in the Cardano knowledge graph.
- For federated governance queries, join SQL governance metadata to SPARQL governance actions by governance_action_tx_id and governance_action_index when available; otherwise join by governance_action_tx_id only and report ambiguity when a transaction contains multiple governance actions.
- Do not use raw_json as the primary query interface unless a field has not yet been normalized.
"""
