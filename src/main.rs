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

    // Initialize the results_map with no_of_periods intervals and 5 price holders
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

    // Fill in initial timestamps
    for _ in 0..no_of_periods {
        results_map.insert(current_timestamp, (None, None, None, None, None));
        current_timestamp = current_timestamp - time_duration;
    }

    println!("Get and Insert Polygon data.. ");
    let polygon_data = get_polygon_data(&time_period, no_of_periods.try_into().unwrap()).await?;
    for (timestamp, vw) in polygon_data {
        // Round the timestamp according to the time_period
        let rounded_timestamp = round_to_period(timestamp, &time_period);
        // Insert or update the rounded timestamp in the HashMap
        results_map
            .entry(rounded_timestamp)
            .and_modify(|e| e.0 = Some(vw))
            .or_insert((Some(vw), None, None, None, None)); // Set Polygon, others remain None
    }

    println!("Get and Insert Dune data..");
    let on_chain_prices = fetch_dune_data(&time_period, no_of_periods.try_into().unwrap()).await?;
    for (day_str, price) in on_chain_prices {
        let day_str = day_str.trim();
        if let Ok(date_time) = NaiveDateTime::parse_from_str(&day_str, "%Y-%m-%d %H:%M:%S%.f %Z") {
            let rounded_timestamp = round_to_period(date_time, &time_period);
            results_map
                .entry(rounded_timestamp)
                .and_modify(|e| e.1 = Some(price))
                .or_insert((None, Some(price), None, None, None)); // Set Dune price, others remain None
        } else {
            println!("Failed to parse date: {}", day_str);
        }
    }

    println!("Get and Insert BitFinex data..");
    let coin_api_data = get_coin_api_data(&time_period).await?;
    for (timestamp, average_price) in coin_api_data {
        let rounded_timestamp = round_to_period(timestamp, &time_period);
        results_map
            .entry(rounded_timestamp)
            .and_modify(|e| e.3 = Some(average_price))
            .or_insert((None, None, None, Some(average_price), None)); // Set BitFinex price, others remain None
    }

    println!("Get and Insert Kraken data.. ");
    let kraken_data = get_kraken_data(&time_period).await?;
    for (timestamp, average_price) in kraken_data {
        let rounded_timestamp = round_to_period(timestamp, &time_period);
        results_map
            .entry(rounded_timestamp)
            .and_modify(|e| e.2 = Some(average_price))
            .or_insert((None, None, Some(average_price), None, None)); // Set Kraken price, others remain None
    }

    /*
    // Convert to a sorted Vec to process and interpolate correctly
    let mut sorted_results: Vec<_> = results_map.into_iter().collect();
    sorted_results.sort_by_key(|&(timestamp, _)| timestamp);

    // Convert sorted_results back to a HashMap for further processing
    let mut sorted_results_map: HashMap<NaiveDateTime, _> = sorted_results.into_iter().collect();
    */

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
