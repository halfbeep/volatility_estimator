use anyhow::Result;
use chrono::{Duration, NaiveDateTime, Utc};
use dotenv::dotenv;
use std::collections::HashMap;
use std::env;

#[path = "./data/coinapi.rs"]
mod coinapi;
use coinapi::get_coin_api_data;

#[path = "./calc/calculate_volatility.rs"]
mod calculate_volatility;
use calculate_volatility::calculate_volatility;

#[path = "./util/rounding.rs"]
mod rounding;
use rounding::round_to_period;

#[path = "./data/dune.rs"]
mod dune;
use dune::fetch_dune_data;

#[path = "./data/kraken.rs"]
mod kraken;
use kraken::get_kraken_data;

#[path = "./data/polygon.rs"]
mod polygon;
use polygon::get_polygon_data;

#[cfg(test)]
#[path = "./calc/calculate_volatility_test.rs"]
mod calculate_volatility_test;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Load the number of periods and the time period from the .env file
    let no_of_periods: usize = env::var("NO_OF_PERIODS")
        .unwrap_or("100".to_string()) // Default to 100 periods if not set
        .parse()
        .expect("NO_OF_PERIODS must be a valid integer");

    // Check that NO_OF_PERIODS is within the required range
    if no_of_periods == 0 || no_of_periods >= 741 {
        return Err(anyhow::anyhow!(
            "NO_OF_PERIODS must be greater than 0 and less than 741"
        ));
    }

    let time_period = env::var("TIME_PERIOD").unwrap_or("hour".to_string());

    // Determine the duration based on TIME_PERIOD
    let time_duration = match time_period.as_str() {
        "second" => Duration::seconds(1),
        "minute" => Duration::minutes(1),
        "hour" => Duration::hours(1),
        "day" => Duration::days(1),
        _ => Duration::hours(1), // Default to 'hour' if the provided value is invalid
    };

    // Initialize the starting timestamp (now - time_period)
    let mut current_timestamp = Utc::now().naive_utc();

    // Initialize a results_map with a 5 price vector
    let mut results_map: HashMap<
        NaiveDateTime,
        (
            Option<f64>, // Dune (Volume Weighted Average Price)
            Option<f64>, // Polygon (Average Price from Dune)
            Option<f64>, // Kraken price
            Option<f64>, // BitFinex price (OHLC Avg)
            Option<f64>, // Placeholder for price used in vol. calc
        ),
    > = HashMap::new();

    // Fill in initial timestamps and create
    // the empty placeholders for the volatility calculation
    for _ in 0..no_of_periods {
        results_map.insert(current_timestamp, (None, None, None, None, None));
        current_timestamp = current_timestamp - time_duration;
    }

    // Attempt to fetch and insert Polygon data
    println!("Fetching Polygon data...");
    match get_polygon_data(&time_period, no_of_periods.try_into().unwrap()).await {
        Ok(polygon_data) => {
            for (timestamp, vw) in polygon_data {
                let rounded_timestamp = round_to_period(timestamp, &time_period);
                results_map
                    .entry(rounded_timestamp)
                    .and_modify(|e| e.0 = Some(vw))
                    .or_insert((Some(vw), None, None, None, None)); // Set Polygon, others remain None
            }
        }
        Err(e) => {
            println!("Failed to fetch Polygon data: {}", e);
        }
    }

    // Attempt to fetch and insert Dune data
    println!("Fetching Dune data...");
    match fetch_dune_data(&time_period, no_of_periods.try_into().unwrap()).await {
        Ok(on_chain_prices) => {
            for (day_str, price) in on_chain_prices {
                let day_str = day_str.trim();
                if let Ok(date_time) =
                    NaiveDateTime::parse_from_str(&day_str, "%Y-%m-%d %H:%M:%S%.f %Z")
                {
                    let rounded_timestamp = round_to_period(date_time, &time_period);
                    results_map
                        .entry(rounded_timestamp)
                        .and_modify(|e| e.1 = Some(price))
                        .or_insert((None, Some(price), None, None, None)); // Set Dune price, others remain None
                } else {
                    println!("Failed to parse date: {}", day_str);
                }
            }
        }
        Err(e) => {
            println!("Failed to fetch Dune data: {}", e);
        }
    }

    // Attempt to fetch and insert BitFinex data
    println!("Fetching BitFinex data...");
    match get_coin_api_data(&time_period).await {
        Ok(coin_api_data) => {
            for (timestamp, average_price) in coin_api_data {
                let rounded_timestamp = round_to_period(timestamp, &time_period);
                results_map
                    .entry(rounded_timestamp)
                    .and_modify(|e| e.3 = Some(average_price))
                    .or_insert((None, None, None, Some(average_price), None)); // Set BitFinex price, others remain None
            }
        }
        Err(e) => {
            println!("Failed to fetch BitFinex data: {}", e);
        }
    }

    // Attempt to fetch and insert Kraken data
    println!("Fetching Kraken data...");
    match get_kraken_data(&time_period).await {
        Ok(kraken_data) => {
            for (timestamp, average_price) in kraken_data {
                let rounded_timestamp = round_to_period(timestamp, &time_period);
                results_map
                    .entry(rounded_timestamp)
                    .and_modify(|e| e.2 = Some(average_price))
                    .or_insert((None, None, Some(average_price), None, None)); // Set Kraken price, others remain None
            }
        }
        Err(e) => {
            println!("Failed to fetch Kraken data: {}", e);
        }
    }

    // Calculate and print volatility
    if let Some(volatility) = calculate_volatility(&mut results_map, no_of_periods, &time_period) {
        // Reorder the map to print it in order
        let mut sorted_results: Vec<_> = results_map.iter().collect();
        sorted_results.sort_by_key(|&(timestamp, _)| timestamp);

        // Print the ordered map with populated and interpolated vol values
        for (timestamp, (vw, ap, kr, ca, vol)) in sorted_results {
            println!(
                "Timestamp: {}, Dune: {:?}, Polygon: {:?}, Kraken: {:?}, BitFinex: {:?}, VOL: {:?}",
                timestamp, vw, ap, kr, ca, vol
            );
        }

        println!(
            "Combined Vol of {} {} bars, avg & volume weighted = {:.6}",
            no_of_periods, time_period, volatility
        );
    } else {
        println!("No data available to calculate volatility.");
    }

    Ok(())
}
