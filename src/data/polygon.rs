use anyhow::Result;
use chrono::{Duration, NaiveDateTime, TimeZone, Utc};
use dotenv::dotenv;
use reqwest::Client;
use serde::Deserialize;
use std::env;

#[derive(Deserialize, Debug)]
struct ApiResponse {
    results: Vec<DataPoint>,
}

#[derive(Deserialize, Debug)]
struct DataPoint {
    vw: f64, // Volume Weighted Average Price (vw)
    t: i64,  // Unix timestamp in milliseconds
}

pub async fn get_polygon_data(
    timespan: &str,
    no_of_periods: i64,
) -> Result<Vec<(NaiveDateTime, f64)>, anyhow::Error> {
    dotenv().ok(); // Load environment variables

    // Load the API key and URL from the environment
    let api_key = env::var("POLYGON_API_KEY").expect("API_KEY not found in .env");
    let api_url = env::var("POLYGON_API_URL").expect("API_URL not found in .env");

    let client = Client::new();

    // Set multiplier to 1
    let multiplier = 1;

    // Calculate the start and end dates based on timespan and no_of_periods
    let end_date = Utc::now();
    let start_date = match timespan {
        "second" => end_date - Duration::seconds(no_of_periods),
        "minute" => end_date - Duration::minutes(no_of_periods),
        "hour" => end_date - Duration::hours(no_of_periods),
        "day" => end_date - Duration::days(no_of_periods),
        _ => return Err(anyhow::anyhow!("Invalid timespan provided")), // Return an error for invalid timespan
    };

    // Format the dates as required by the Polygon API (in this case, assuming "YYYY-MM-DD")
    let start_date_str = start_date.format("%Y-%m-%d").to_string();
    let end_date_str = end_date.format("%Y-%m-%d").to_string();

    // Build the query URL
    let query_url = format!(
        "{}/{}/{}/{}/{}?apiKey={}",
        api_url, multiplier, timespan, start_date_str, end_date_str, api_key
    );

    // Debug
    // println!("Url: {}", query_url);

    // Make the HTTP request to the Polygon API
    let response = client
        .get(&query_url)
        .send()
        .await?
        .json::<ApiResponse>()
        .await?;

    // Debug
    // println!("Responses: {:?}", response);

    // Parse the data into (NaiveDateTime, f64)
    let parsed_data: Vec<(NaiveDateTime, f64)> = response
        .results
        .iter()
        .filter_map(|data_point| {
            // Convert the timestamp (t) from milliseconds to seconds using Utc
            let timestamp = Utc.timestamp_opt(data_point.t / 1000, 0).single()?;
            Some((timestamp.naive_utc(), data_point.vw))
        })
        .collect();

    Ok(parsed_data)
}
