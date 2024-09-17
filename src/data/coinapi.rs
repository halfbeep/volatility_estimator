use anyhow::Result;
use chrono::NaiveDateTime;
use log::{debug, error};
use reqwest::StatusCode;
use serde::Deserialize;
use std::env;
use tokio::time::{timeout, Duration};

// Define a struct for CoinAPI response
#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct CoinApiRecord {
    time_period_start: String,
    price_open: f64,
    price_high: f64,
    price_low: f64,
    price_close: f64,
    volume_traded: f64,
    trades_count: u64,
}

pub async fn get_coin_api_data(
    time_period: &str,
) -> Result<Vec<(NaiveDateTime, f64)>, anyhow::Error> {
    let asset_id = "BITFINEX_SPOT_ETH_USD";

    // Convert timespan to period
    let period = match time_period {
        "second" => "1SEC",
        "minute" => "1MIN",
        "hour" => "1HRS",
        "day" => "1DAY",
        _ => {
            error!("Unsupported timespan provided");
            return Err(anyhow::anyhow!("Unsupported timespan provided"));
        }
    };

    // Load the CoinAPI key from .env
    let api_key = env::var("COINAPI_API_KEY").expect("COINAPI_API_KEY must be set in .env file");

    // Construct the actual URL
    let url = format!(
        "https://rest.coinapi.io/v1/ohlcv/{}/history?period_id={}",
        asset_id, period
    );

    debug!("Constructed CoinAPI URL: {}", url);

    // Make the request to CoinAPI for BitFinex
    let client = reqwest::Client::new();

    let timeout_duration = Duration::from_secs(10);
    debug!("Sending request to CoinAPI...");

    let response = timeout(timeout_duration, async {
        client
            .get(&url)
            .header("X-CoinAPI-Key", &api_key)
            .header("ACCEPT", "application/json")
            .send()
            .await
    })
    .await;

    debug!("Received response from CoinAPI");

    match response {
        Ok(Ok(response)) => {
            // Debug
            debug!("Received response from CoinAPI");

            // Check if the response is successful
            if response.status() != StatusCode::OK {
                let status = response.status();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                error!(
                    "Request failed with status: {} and body: {}",
                    status, error_text
                );
                return Err(anyhow::anyhow!(
                    "CoinAPI request failed with status {}",
                    status
                ));
            }

            // Deserialize the response directly into a Vec<CoinApiRecord>
            let records: Vec<CoinApiRecord> = response.json().await?;
            debug!("Parsed CoinAPI response successfully");

            // Convert the deserialized records into the expected Vec<(NaiveDateTime, f64)>
            let exchange_rates: Vec<(NaiveDateTime, f64)> = records
                .into_iter()
                .filter_map(|record| {
                    // Convert `time_period_start` to `NaiveDateTime`
                    let datetime = NaiveDateTime::parse_from_str(
                        &record.time_period_start,
                        "%Y-%m-%dT%H:%M:%S%.fZ",
                    )
                    .ok()?;

                    // Calculate the average of open, high, low, and close prices
                    let average_price = (record.price_open
                        + record.price_high
                        + record.price_low
                        + record.price_close)
                        / 4.0;

                    Some((datetime, average_price))
                })
                .collect();

            Ok(exchange_rates)
        }
        Ok(Err(e)) => {
            error!("Error sending request to CoinAPI: {}", e);
            Err(anyhow::anyhow!("Error sending request to CoinAPI: {}", e))
        }
        Err(_) => {
            error!("Timeout occurred while trying to fetch data from CoinAPI");
            Err(anyhow::anyhow!(
                "Timeout occurred while trying to fetch data from CoinAPI"
            ))
        }
    }
}
