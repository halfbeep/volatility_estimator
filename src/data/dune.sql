-- in a dune account, create 4 individual queries like the example in src/data/dune.sql
-- on saving these queries you will have 4 unique query_id's for the .env file :-
-- DUNE_QUERY_ID_SECOND=2122120
-- DUNE_QUERY_ID_MIN=7654321
-- DUNE_QUERY_ID_HOUR=7117110
-- DUNE_QUERY_ID_DAY=1234567

-- HOUR EXAMPLE BELOW

WITH eth_prices AS (
  SELECT
    block_time AS timestamp,
    token_sold_amount / token_bought_amount AS eth_price_in_usd -- ETH price in USD
  FROM
   uniswap_v3_ethereum.trades
  WHERE
    -- USDC is token0 and WETH is token1
    token_sold_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eb48 -- USDC contract address
    AND token_bought_address = 0xC02aaa39b223FE8D0A0e5C4F27eAD9083C756Cc2 -- WETH contract address
    AND block_time > NOW() - interval '750' hour
)
SELECT
  date_trunc('hour', timestamp) AS tspan,
  avg(eth_price_in_usd) AS average_eth_price
FROM
  eth_prices
GROUP BY 1
ORDER BY 1 DESC;

-- MINUTE EXAMPLE BELOW

WITH eth_prices AS (
  SELECT
    block_time AS timestamp,
    token_sold_amount / token_bought_amount AS eth_price_in_usd -- ETH price in USD
  FROM
   uniswap_v3_ethereum.trades
  WHERE
    -- USDC is token0 and WETH is token1
    token_sold_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eb48 -- USDC contract address
    AND token_bought_address = 0xC02aaa39b223FE8D0A0e5C4F27eAD9083C756Cc2 -- WETH contract address
    AND block_time > NOW() - interval '741' minute
)
SELECT
  date_trunc('minute', timestamp) AS tspan,
  avg(eth_price_in_usd) AS average_eth_price
FROM
  eth_prices
GROUP BY 1
ORDER BY tspan DESC;