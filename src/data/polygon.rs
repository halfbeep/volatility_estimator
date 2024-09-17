use anyhow::{anyhow, Result};
use chrono::{Duration, NaiveDateTime, Utc}; // Make sure to import chrono::Duration
use log::{debug, error};
use reqwest::Client;
use serde::Deserialize;
use std::env;
use std::time::Duration as StdDuration; // Rename to avoid conflict with `chrono::Duration`

#[derive(Deserialize, Debug)]
struct PolygonApiResponse {
    results: Option<Vec<PolygonData>>,
}

#[derive(Deserialize, Debug)]
struct ErrorResponse {
    status: String,
    message: String,
}

#[derive(Deserialize, Debug)]
struct PolygonData {
    #[serde(rename = "t")]
    timestamp: i64, // Unix timestamp in milliseconds
    #[serde(rename = "vw")]
    vw: f64, // Volume-weighted average price
}

pub async fn get_polygon_data(
    time_period: &str,
    no_of_periods: i64,
) -> Result<Vec<(NaiveDateTime, f64)>, anyhow::Error> {
    let asset_id = "X:ETHUSD";

    // Set the API key and URL
    let api_key = env::var("POLYGON_API_KEY").expect("POLYGON_API_KEY not found in .env");

    debug!("Api key: {}", api_key);

    let api_url = format!("https://api.polygon.io/v2/aggs/ticker/{}/range", asset_id);

    debug!("Api url: {}", api_url);

    let client = Client::new();

    // Default multiplier to 1
    let multiplier = 1;

    // Calculate the start and end dates for API based on timespan and no_of_periods
    let end_date = Utc::now();
    let start_date = match time_period {
        "second" => end_date - Duration::seconds(no_of_periods),
        "minute" => end_date - Duration::minutes(no_of_periods),
        "hour" => end_date - Duration::hours(no_of_periods),
        "day" => end_date - Duration::days(no_of_periods),
        _ => return Err(anyhow!("Invalid timespan provided")), // Return an error for invalid timespan
    };

    // Format the dates as required by API (in this case, assuming "YYYY-MM-DD")
    let start_date_str = start_date.format("%Y-%m-%d").to_string();
    let end_date_str = end_date.format("%Y-%m-%d").to_string();

    debug!("Start date string: {}", start_date_str);
    debug!("End date string: {}", end_date_str);

    // Build the final query URL
    let url = format!(
        "{}/{}/{}/{}/{}?apiKey={}",
        api_url, multiplier, time_period, start_date_str, end_date_str, api_key
    );

    debug!("Polygon API request URL: {}", url);

    let response = client
        .get(&url)
        .timeout(StdDuration::from_secs(10)) // Set a timeout for the request
        .send()
        .await;

    match response {
        Ok(resp) => {
            debug!("Received response: {:?}", resp);
            if !resp.status().is_success() {
                // Attempt to parse the error response
                let status_code = resp.status();
                let error_text = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read error response".to_string());

                // Try to deserialize the error response
                if let Ok(error_response) = serde_json::from_str::<ErrorResponse>(&error_text) {
                    error!(
                        "Polygon API request failed with status: {}. Error: {}",
                        status_code, error_response.message
                    );

                    // Handle specific error case for plan limitations
                    if status_code == reqwest::StatusCode::FORBIDDEN
                        && error_response.status == "NOT_AUTHORIZED"
                    {
                        return Err(anyhow!(
                            "Polygon API request failed due to plan limitations: {} - {}. Consider upgrading your plan at https://polygon.io/pricing",
                            status_code,
                            error_response.message
                        ));
                    }

                    return Err(anyhow!(
                        "Polygon API request failed: {} - {}",
                        status_code,
                        error_response.message
                    ));
                } else {
                    // If deserialization fails, return a generic error
                    error!(
                        "Polygon API request failed with status: {}. Response: {}",
                        status_code, error_text
                    );
                    return Err(anyhow!(
                        "Polygon API request failed with status: {}. Response: {}",
                        status_code,
                        error_text
                    ));
                }
            }

            // Parse the JSON response
            let api_response: PolygonApiResponse = resp.json().await?;

            // Check if the results field is present and non-empty
            if let Some(data) = api_response.results {
                if data.is_empty() {
                    error!("Polygon API returned an empty results array.");
                    return Err(anyhow!("Polygon API returned an empty results array"));
                }

                let parsed_data: Vec<(NaiveDateTime, f64)> = data
                    .into_iter()
                    .filter_map(|d| {
                        #[allow(deprecated)]
                        NaiveDateTime::from_timestamp_opt(d.timestamp / 1000, 0)
                            .map(|naive_dt| (naive_dt, d.vw))
                    })
                    .collect();

                if parsed_data.is_empty() {
                    error!("Parsed data is empty after processing Polygon API response.");
                    return Err(anyhow!(
                        "Parsed data is empty after processing Polygon API response"
                    ));
                }

                debug!("Parsed data: {:?}", parsed_data);
                Ok(parsed_data)
            } else {
                error!("No results field in Polygon API response.");
                Err(anyhow!("No results in Polygon API response"))
            }
        }
        Err(e) => {
            error!("Failed to send request to Polygon API: {}", e);
            Err(anyhow!("Failed to send request to Polygon API: {}", e))
        }
    }
}
