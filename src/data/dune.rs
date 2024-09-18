use anyhow::{anyhow, Result};
use dotenv::dotenv;
use log::debug;
use reqwest::Client;
use serde::{Deserialize, Deserializer};
use serde_json;
use std::env;

#[derive(Deserialize, Debug)]
struct DuneAnalyticsResponse {
    result: DuneResult,
}

#[derive(Deserialize, Debug)]
struct DuneResult {
    rows: Vec<CryptoPriceDataRaw>,
}

#[derive(Deserialize, Debug)]
struct CryptoPriceDataRaw {
    tspan: String,
    #[serde(deserialize_with = "deserialize_price")]
    average_eth_price: f64,
}

// Enum to handle both string and float values
#[derive(Debug, Deserialize)]
#[serde(untagged)] // Automatically handle deserialization of both string and float
enum Price {
    String(String),
    Float(f64),
}

// Data Cleansing to remove 'crazy' returns from feed
// Custom deserialization to handle "Infinity" and other non-numeric values
fn deserialize_price<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let price = Price::deserialize(deserializer)?; // Use built-in deserialization
    match price {
        Price::String(s) => match s.as_str() {
            "Infinity" | "-Infinity" | "NaN" => Ok(f64::INFINITY), // Treat as infinity
            _ => s.parse::<f64>().map_err(serde::de::Error::custom), // Handle numeric strings
        },
        Price::Float(f) => Ok(f), // Handle floats directly
    }
}

// Function to fetch price data from Dune Analytics
pub async fn fetch_dune_data(
    timespan: &str,
    no_of_periods: i64,
) -> Result<Vec<(String, f64)>, anyhow::Error> {
    dotenv().ok(); // Load environment variables

    // Load the appropriate query ID based on the timespan
    let query_id = match timespan {
        "second" => {
            env::var("DUNE_QUERY_ID_SEC").expect("DUNE_QUERY_ID_SEC must be set in .env file")
        }
        "minute" => {
            env::var("DUNE_QUERY_ID_MIN").expect("DUNE_QUERY_ID_MIN must be set in .env file")
        }
        "hour" => {
            env::var("DUNE_QUERY_ID_HOUR").expect("DUNE_QUERY_ID_HOUR must be set in .env file")
        }
        "day" => env::var("DUNE_QUERY_ID_DAY").expect("DUNE_QUERY_ID_DAY must be set in .env file"),
        _ => return Err(anyhow!("Unsupported timespan provided for Dune data")), // Return an error for unsupported timespans
    };

    let api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY must be set in .env file");

    debug!("Api key: {}", api_key);

    // Dune Analytics API URL with the provided query ID
    let url = format!(
        "https://api.dune.com/api/v1/query/{}/results?limit={}",
        query_id, no_of_periods
    );

    debug!("Dune Url: {}", url);

    // Make the API call
    let client = Client::new();
    let raw_response = client
        .get(&url)
        .header("X-Dune-API-Key", api_key)
        .send()
        .await;

    // Check if the request succeeded
    match raw_response {
        Ok(response) => {
            let response_text = response.text().await?;
            // Deserialize response into expected struct
            let response_data: DuneAnalyticsResponse = serde_json::from_str(&response_text)
                .map_err(|e| anyhow!("Failed to deserialize response: {}", e))?;

            debug!("Response: {:?}", response_data);

            // Data Cleansing
            // Extract prices and filter out non-finite values (Infinity, NaN, etc.)
            let prices: Vec<f64> = response_data
                .result
                .rows
                .iter()
                .map(|row| row.average_eth_price)
                .filter(|&price| price.is_finite()) // Filter out Infinity and NaN values
                .collect();

            // Calculate the average of finite prices
            if prices.is_empty() {
                return Err(anyhow!("No valid prices found"));
            }
            let avg: f64 = prices.iter().sum::<f64>() / prices.len() as f64;
            debug!("Average price {}", avg);

            // More Data Cleansing and preparation for vol calcs
            // Filter ETH prices that are greater than $8000
            let filtered_prices: Vec<(String, f64)> = response_data
                .result
                .rows
                .into_iter()
                .filter_map(|row| {
                    if row.average_eth_price <= 8000.0 {
                        Some((row.tspan, row.average_eth_price))
                    } else {
                        None
                    }
                })
                .collect();

            debug!("Filtered prices: {:?}", filtered_prices);

            Ok(filtered_prices)
        }
        Err(e) => {
            // If the request failed, print the error and return it
            eprintln!("Request to Dune Analytics failed: {}", e);
            Err(anyhow!(e))
        }
    }
}
