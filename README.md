## Volatility Estimator

**(uses on-chain & off-chain prices)**

$${V}_{x} = \sqrt{\frac{1}{N-1} \sum_{i=1}^N (x_i - \overline{x})^2}$$

    git clone [this repo]
    cd volatility_estimator/
    cargo test

If the tests are all ok then populate the API keys and Dune query_id's to populate the .env file (pre populated to 100 periods, period 1 hour)

    cargo run

Dune Queries: in a dune account, create 4 individual queries like the example here .in src/data/dune.sql. On saving the queries you will have 4 unique query_id's for this .env

-- DUNE_QUERY_ID_SECOND=2122120
-- DUNE_QUERY_ID_MIN=7654321
-- DUNE_QUERY_ID_HOUR=7117110
-- DUNE_QUERY_ID_DAY=1234567

(click on 'API' in bottom right of SQL window to reveal API query id)
