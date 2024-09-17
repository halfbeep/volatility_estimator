use anyhow::Result;
use chrono::{Duration, NaiveDateTime, Utc};
use dotenv::dotenv;
use log::debug;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, RwLock};
use tokio::task;

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
mod polygon2;
use polygon2::get_polygon_data;

#[cfg(test)]
#[path = "./calc/calculate_volatility_test.rs"]
mod calculate_volatility_test;

type ResultsMap = Arc<
    RwLock<
        HashMap<
            NaiveDateTime,
            (
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
                Option<f64>,
            ),
        >,
    >,
>;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger once at the start of the program
    if env_logger::try_init().is_err() {
        eprintln!("Logger was already initialized");
    }
    dotenv().ok();

    // Load the number of periods from the .env file
    let no_of_periods: usize = env::var("NO_OF_PERIODS")
        .unwrap_or("100".to_string()) // Default to 100 periods if not set
        .parse()
        .expect("NO_OF_PERIODS must be a valid integer");

    // Check that NO_OF_PERIODS is in a reasonable range
    if no_of_periods == 0 || no_of_periods >= 741 {
        return Err(anyhow::anyhow!(
            "NO_OF_PERIODS must be greater than 0 and less than 741"
        ));
    }

    // Default to hour if period is absent
    let time_period = env::var("TIME_PERIOD").unwrap_or("hour".to_string());
    // Validate that TIME_PERIOD is one of "second", "minute", "hour", or "day"
    if !["second", "minute", "hour", "day"].contains(&time_period.as_str()) {
        return Err(anyhow::anyhow!(
            "TIME_PERIOD must be one of: 'second', 'minute', 'hour', or 'day'."
        ));
    }

    // Convert `no_of_periods` to `i64`
    let no_of_periods_i64 = no_of_periods.try_into().unwrap();

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
    // (includes price 'VOLPrice' used for calculation)
    let results_map: ResultsMap = Arc::new(RwLock::new(HashMap::new()));

    // Fill in initial timestamps, creating
    // the placeholders for the volatility estimate
    {
        let mut map = results_map.write().unwrap();
        for _ in 0..no_of_periods {
            // Round the current timestamp to the specified time period
            let rounded_timestamp = round_to_period(current_timestamp, &time_period);

            // Insert the rounded timestamp into the map with default values
            map.insert(rounded_timestamp, (None, None, None, None, None));

            // Move to the previous time period
            current_timestamp = current_timestamp - time_duration;

            // Debug output to verify the timestamps
            debug!("{}", rounded_timestamp);
        }
    }

    // THread safe for multiple sources with different response times
    // Spawn tasks to fetch data asynchronously and update the results_map
    let polygon_map = Arc::clone(&results_map);
    let polygon_time_period = time_period.clone();
    let polygon_task = tokio::spawn(async move {
        println!("Fetching Polygon data...");
        if let Err(e) = get_polygon_data(&polygon_time_period, no_of_periods_i64)
            .await
            .map(|polygon_data| {
                // Store the results in a local vector first
                let rounded_entries: Vec<_> = polygon_data
                    .into_iter()
                    .map(|(timestamp, vw)| {
                        let rounded_time = round_to_period(timestamp, &polygon_time_period);
                        (rounded_time, vw)
                    })
                    .collect();
                // Lock the map only when updating it
                {
                    let mut map = polygon_map.write().unwrap();
                    for (rounded_time, vw) in rounded_entries {
                        if map.contains_key(&rounded_time) {
                            debug!("Updating existing entry: {} with vw: {}", rounded_time, vw);
                        } else {
                            debug!("Inserting new entry: {} with vw: {}", rounded_time, vw);
                        }

                        map.entry(rounded_time)
                            .and_modify(|e| e.0 = Some(vw))
                            .or_insert((Some(vw), None, None, None, None)); // Set Polygon, others remain None
                    }

                    debug!("Map after insertion: {:?}", *map);
                } // Lock is released here
            })
        {
            println!("Failed to fetch Polygon data: {:?}", e);
        }
    });

    // Await the polygon_task separately
    if let Err(e) = polygon_task.await {
        println!("Polygon task failed to complete: {:?}", e);
    }

    // Now proceed to start the Dune data fetch
    let dune_map = Arc::clone(&results_map);
    let dune_time_period = time_period.clone();
    let dune_task = tokio::spawn(async move {
        println!("Fetching Dune data...");
        if let Err(e) = fetch_dune_data(&dune_time_period, no_of_periods.try_into().unwrap())
            .await
            .map(|dune_prices| {
                {
                    let mut map = dune_map.write().unwrap();
                    for (day_str, aprice) in dune_prices {
                        if let Ok(timestamp) =
                            NaiveDateTime::parse_from_str(&day_str, "%Y-%m-%d %H:%M:%S%.f %Z")
                        {
                            let rounded_time = round_to_period(timestamp, &dune_time_period);
                            debug!("Dune Time & Price: {}   {}", rounded_time, aprice);
                            map.entry(rounded_time)
                                .and_modify(|e| e.1 = Some(aprice))
                                .or_insert((None, Some(aprice), None, None, None));
                        } else {
                            println!("Skipping insertion due to invalid timestamp.");
                        }
                    }
                } // Lock is released here !
            })
        {
            println!("Failed to fetch Dune data: {:?}", e);
        }
    });

    // Await the dune_task separately
    if let Err(e) = dune_task.await {
        println!("Dune task failed to complete: {:?}", e);
    }

    let kraken_map = Arc::clone(&results_map);
    let kraken_time_period = time_period.clone();
    let kraken_task = task::spawn(async move {
        println!("Fetching Kraken data...");
        match get_kraken_data(&kraken_time_period).await {
            Ok(kraken_data) => {
                let mut map = kraken_map.write().unwrap();
                for (timestamp, average_price) in kraken_data {
                    let rounded_timestamp = round_to_period(timestamp, &kraken_time_period);
                    map.entry(rounded_timestamp)
                        .and_modify(|e| e.2 = Some(average_price))
                        .or_insert((None, None, Some(average_price), None, None));
                    // Set Kraken price, others remain None
                }
            } // Lock is Released here
            Err(e) => {
                println!("Failed to fetch Kraken data: {}", e);
            }
        }
    });

    // Await the dune_task separately
    if let Err(e) = kraken_task.await {
        println!("Kraken task failed to complete: {:?}", e);
    }

    let coin_api_map = Arc::clone(&results_map);
    let coin_api_time_period = time_period.clone();
    let coin_api_task = task::spawn(async move {
        println!("Fetching Coin API data...");
        match get_coin_api_data(&coin_api_time_period).await {
            Ok(coin_api_data) => {
                let mut map = coin_api_map.write().unwrap();
                for (timestamp, average_price) in coin_api_data {
                    let rounded_timestamp = round_to_period(timestamp, &coin_api_time_period);
                    map.entry(rounded_timestamp)
                        .and_modify(|e| e.3 = Some(average_price))
                        .or_insert((None, None, None, Some(average_price), None));
                    // Set BitFinex price, others remain None
                }
            } // Lock released here
            Err(e) => {
                println!("Failed to fetch Coin API data: {}", e);
            }
        }
    });

    // Await the dune_task separately
    if let Err(e) = coin_api_task.await {
        println!("CoinAPI task failed to complete: {:?}", e);
    }

    // Calculate volatility, then print all the sata
    let time_period_ref = time_period.clone(); // Use a reference for `time_period` here
    if let Some(volatility) = calculate_volatility(
        &results_map, // Pass the Arc<RwLock<...>> reference only
        no_of_periods,
    ) {
        // Reorder the map to print
        let map_read = results_map.read().unwrap(); // Longer-lived binding
        let mut sorted_results: Vec<_> = map_read.iter().collect();
        sorted_results.sort_by_key(|&(timestamp, _)| timestamp);

        // Print the ordered map with populated and interpolated final 'useabble' price values
        for (timestamp, (vw, ap, kr, ca, vol)) in sorted_results {
            println!(
                "Timestamp: {}, Polygon: {:?}, Dune: {:?}, Kraken: {:?}, BitFinex: {:?}, VOL_Price: {:?}",
                timestamp, vw, ap, kr, ca, vol
            );
        }

        println!(
            "Estimated Volatility over last {} {} bars, ohlc avg & volume weighted = {:.6}",
            no_of_periods, time_period_ref, volatility
        );
    } else {
        println!("No data available to calculate volatility.");
    }

    Ok(())
}
