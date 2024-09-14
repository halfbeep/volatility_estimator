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
ORDER BY block_time
LIMIT 100000
)
SELECT
  date_trunc('minute', timestamp) AS minute,
  avg(eth_price_in_usd) AS min_average_eth_price
FROM
  eth_prices
GROUP BY
  1
ORDER BY
  1;