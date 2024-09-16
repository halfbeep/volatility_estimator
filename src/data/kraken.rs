use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

// Define a struct to hold the response from the Kraken API
#[derive(Deserialize, Debug)]
struct KrakenApiResponse {
    result: KrakenResult,
}

#[derive(Deserialize, Debug)]
struct KrakenResult {
    #[serde(rename = "last")]
    _last: Value, // We don't need this field, so use `Value` to skip it
    #[serde(flatten)]
    ohlc: HashMap<String, Vec<Vec<Value>>>,
}

// Function to fetch Kraken OHLC data
pub async fn get_kraken_data(
    time_period: &str,
) -> Result<Vec<(NaiveDateTime, f64)>, anyhow::Error> {
    let asset_id = "ETHPYUSD";

    // Convert time_period to the correct interval in minutes for Kraken API
    let interval_minutes = match time_period {
        "minute" => 1, // 1 minute
        "hour" => 60,  // 60 minutes
        "day" => 1440, // 1440 minutes (24 hours)
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported time period provided for Kraken data"
            ))
        } // Return an error for unsupported time periods (like seconds)
    };

    // Construct the actual URL
    let url = format!(
        "https://api.kraken.com/0/public/OHLC?pair={}&interval={}",
        asset_id, interval_minutes
    );

    let client = Client::new();

    // Make the request to API
    let response = client
        .get(&url)
        .send()
        .await?
        .json::<KrakenApiResponse>()
        .await?;

    // Debug
    // println!("Responses: {:?}", response);

    // Extract OHLC data
    let ohlc_data = response
        .result
        .ohlc
        .get(asset_id)
        .ok_or_else(|| anyhow::anyhow!("No OHLC data found for the specified pair"))?;

    // Parse the average of OHLC into vec(NaiveDateTime, f64)
    let parsed_ohlc: Vec<(NaiveDateTime, f64)> = ohlc_data
        .iter()
        .filter_map(|ohlc| {
            if ohlc.len() < 5 {
                return None; // Ensure OHLC has enough fields (timestamp, o, h, l, c)
            }

            // ohlc[0] contains the timestamp, which may be an integer or string
            let timestamp = match &ohlc[0] {
                Value::Number(n) => n.as_i64().unwrap_or(0),
                Value::String(s) => s.parse::<i64>().unwrap_or(0),
                _ => return None,
            };

            // Convert timestamp to NaiveDateTime using Utc and then naive_utc
            let datetime = Utc.timestamp_opt(timestamp, 0).single()?.naive_utc();

            // ohlc[1] to ohlc[4] contain the open, high, low, and close prices
            let open_price = match &ohlc[1] {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                _ => return None,
            };

            let high_price = match &ohlc[2] {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                _ => return None,
            };

            let low_price = match &ohlc[3] {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                _ => return None,
            };

            let close_price = match &ohlc[4] {
                Value::Number(n) => n.as_f64().unwrap_or(0.0),
                Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
                _ => return None,
            };

            // Calculate the average price
            let average_price = (open_price + high_price + low_price + close_price) / 4.0;

            Some((datetime, average_price))
        })
        .collect();

    Ok(parsed_ohlc)
}
