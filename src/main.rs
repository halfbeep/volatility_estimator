use anyhow::Result;
use chrono::{Duration, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use dotenv::dotenv;
use on_chain::fetch_dune_data;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;

mod calculate_volatility;
mod on_chain;
use crate::calculate_volatility::calculate_volatility;
#[cfg(test)]
mod calculate_volatility_test; // Include the test module only during testing

#[derive(Deserialize, Debug)]
struct ApiResponse {
    results: Vec<DataPoint>,
}

#[derive(Deserialize, Debug)]
struct DataPoint {
    vw: f64, // Volume Weighted Average Price (vw)
    t: i64,  // Unix timestamp in milliseconds
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let dune_api_key = env::var("DUNE_API_KEY").expect("DUNE_API_KEY must be set in .env file");
    let dune_query_id =
        env::var("DUNE_QUERY_ID_HOUR").expect("DUNE_QUERY_ID must be set in .env file");

    // Load the API key, URL, and time period from the environment
    let api_key = env::var("POLYGON_API_KEY").expect("API_KEY not found in .env");
    let api_url = env::var("POLYGON_API_URL").expect("API_URL not found in .env");

    // Load the time period from the .env file
    let time_period_days: i64 = env::var("TIME_PERIOD_DAYS")
        .unwrap_or("1".to_string()) // Default to 1 day if not set
        .parse()
        .expect("TIME_PERIOD_DAYS must be a valid integer");

    // Load VOLATILITY_PERIOD from the .env file
    let volatility_period_days: usize = env::var("VOLATILITY_PERIOD")
        .unwrap_or("30".to_string()) // Default to 30 days if not set
        .parse()
        .expect("VOLATILITY_PERIOD must be a valid integer");

    // Load MULTIPLIER and TIMESPAN from the .env file
    let multiplier: i64 = env::var("MULTIPLIER")
        .unwrap_or("1".to_string()) // Default to 1 if not set
        .parse()
        .expect("MULTIPLIER must be a valid integer");

    let timespan = env::var("TIMESPAN").unwrap_or("minute".to_string()); // Default to minute if not set

    // Get the current date
    let today = Utc::now().date_naive(); // Get today's date without time
    let end_date = today - Duration::days(0); // Today
    let start_date = end_date - Duration::days(time_period_days); // Go back `TIME_PERIOD_DAYS` before the end date

    // Format the dates as YYYY-MM-DD (without time)
    let start_date_str = start_date.format("%Y-%m-%d").to_string();
    let end_date_str = end_date.format("%Y-%m-%d").to_string();

    // Build the query URL
    let query_url = format!(
        "{}/{}/{}/{}/{}?apiKey={}",
        api_url, multiplier, timespan, start_date_str, end_date_str, api_key
    );

    // Make the HTTP request to the API
    let polygon_response = reqwest::get(&query_url)
        .await?
        .json::<ApiResponse>()
        .await?;

    // Create a map to store the results, keyed by rounded timestamp, with VW and AP as Option<f64>
    let mut results_map: HashMap<NaiveDateTime, (Option<f64>, Option<f64>)> = HashMap::new();

    for data_point in polygon_response.results {
        // Convert the timestamp (t) from milliseconds to seconds using `timestamp_opt`
        let timestamp = Utc
            .timestamp_opt(data_point.t / 1000, 0)
            .single() // Extracts the `DateTime` from the `Option`, returning None if invalid
            .expect("Invalid timestamp")
            .naive_utc();

        // Round the timestamp according to the timespan
        let rounded_timestamp = match timespan.as_str() {
            "minute" => round_to_minute(timestamp),
            "hour" => round_to_hour(timestamp),
            "day" => timestamp.date().and_hms_opt(0, 0, 0).unwrap(),
            _ => round_to_minute(timestamp), // Default to minute rounding
        };

        // Insert the rounded timestamp and 'vw' value into the HashMap
        results_map
            .entry(rounded_timestamp)
            .and_modify(|e| e.0 = Some(data_point.vw))
            .or_insert((Some(data_point.vw), None)); // Set VW, AP remains None
    }

    // Make the Dune Analytics API call
    let on_chain_prices = fetch_dune_data(&dune_query_id, &dune_api_key).await?;

    // Process the on-chain prices
    for (day_str, price) in on_chain_prices {
        // Trim whitespace if necessary
        let day_str = day_str.trim();

        // Try parsing the date in the given format
        if let Ok(date_time) = NaiveDateTime::parse_from_str(&day_str, "%Y-%m-%d %H:%M:%S%.f %Z") {
            // Round the timestamp according to the timespan
            let rounded_timestamp = match timespan.as_str() {
                "minute" => round_to_minute(date_time),
                "hour" => round_to_hour(date_time),
                "day" => date_time.date().and_hms_opt(0, 0, 0).unwrap(),
                _ => round_to_minute(date_time), // Default to minute rounding
            };

            // Insert or update the price in the map
            // If the timestamp exists, update only the AP field
            results_map
                .entry(rounded_timestamp)
                .and_modify(|e| e.1 = Some(price))
                .or_insert((None, Some(price))); // Set AP, VW remains None
        } else {
            println!("Failed to parse date: {}", day_str);
        }
    }

    // Sort and collect the map
    let mut sorted_results: Vec<_> = results_map.iter().collect();
    sorted_results.sort_by_key(|&(timestamp, _)| *timestamp);

    // Take only the last N entries based on the VOLATILITY_PERIOD
    let recent_data: Vec<_> = sorted_results
        .iter()
        .rev()
        .take(volatility_period_days)
        .collect();

    // Print the selected results
    for (timestamp, (vw, ap)) in recent_data.iter().rev() {
        // Reverse again to maintain ascending order
        println!("Timestamp: {}, VW: {:?}, AP: {:?}", timestamp, vw, ap);
    }

    // Calculate the volatility (standard deviation) using the VW values
    let vw_values: Vec<f64> = recent_data
        .iter()
        .filter_map(|(_, (vw, _))| *vw) // Filter and collect only VW values that are Some
        .collect();

    // Calculate the volatility
    if let Some(volatility) = calculate_volatility(&vw_values) {
        println!(
            "Polygon/Dune volatility of {} {} bars, volume weighted, for last {} {}s = {:.6}",
            multiplier, timespan, volatility_period_days, timespan, volatility,
        );
    } else {
        println!("No data available to calculate volatility.");
    }

    Ok(())
}

/// Rounds the timestamp to the nearest minute.
fn round_to_minute(timestamp: NaiveDateTime) -> NaiveDateTime {
    let time = timestamp.time();
    let rounded_time = NaiveTime::from_hms_opt(time.hour(), time.minute(), 0).unwrap(); // Set seconds to 0
    NaiveDateTime::new(timestamp.date(), rounded_time)
}

/// Rounds the timestamp to the nearest hour.
fn round_to_hour(timestamp: NaiveDateTime) -> NaiveDateTime {
    let time = timestamp.time();
    let rounded_time = NaiveTime::from_hms_opt(time.hour(), 0, 0).unwrap(); // Set minutes and seconds to 0
    NaiveDateTime::new(timestamp.date(), rounded_time)
}
