## Volatility Estimator

Uses on-chain & off-chain prices on a continuous basis (24/7) from kraken, dune, bitFinex and polygon. Lowest of average or volume weighted prices used with linear interpolation and projection of *N* returns

## $\sigma$ = $\sqrt{\frac{1}{N-1} \sum_{i=1}^N (x_i - \overline{x})^2}$

When using this estimator, 30 days, hours or minutes of prices gives you 29 'returns' used for the volatility. If you want 30 days 'return' specify NO_OF_PERIODS=31 in your .env file

    git clone [this repo]
    cd volatility_estimator/
    cargo test

    cargo run

If the tests pass ok then populate the .env file with your API keys and Dune query_id's

- COINAPI_API_KEY=[your api key]
- POLYGON_API_KEY=[your api key]
- DUNE_API_KEY=[your api key]
- **Dune Queries:** in a dune account, create 4 individual queries like the example here .in src/data/dune.sql. On saving the queries you will create 4 unique query_id's for this .env, like:-
- DUNE_QUERY_ID_SECOND=[your query id]
- DUNE_QUERY_ID_MIN=[your query id]
- DUNE_QUERY_ID_HOUR=[your query id]
- DUNE_QUERY_ID_DAY=[your query id]
  (click on 'API' in bottom right of dune SQL window to reveal API query_id in URL)
