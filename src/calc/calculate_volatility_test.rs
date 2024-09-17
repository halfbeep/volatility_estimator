use super::calculate_volatility::calculate_volatility;
use chrono::{Duration, NaiveDateTime, Utc};
use rand::Rng; // Import random number generation
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[test]
fn test_calculate_volatility_with_random_prices() {
    let mut rng = rand::thread_rng();

    // Create an Arc<RwLock<HashMap>> to store the random prices
    let results_map: Arc<
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
    > = Arc::new(RwLock::new(HashMap::new()));

    // Generate 50 random prices between 50.0 and 150.0
    for i in 0..50 {
        let timestamp = Utc::now().naive_utc() - Duration::seconds(i);
        let random_price = rng.gen_range(50.0..150.0);

        // Insert the random price into the HashMap within the RwLock
        {
            let mut map = results_map.write().unwrap();
            map.insert(timestamp, (Some(random_price), None, None, None, None));
        }
    }

    // Set the number of periods for vol calculation
    let no_of_periods = 50;

    // Call the volatility calculation function
    match calculate_volatility(&results_map, no_of_periods) {
        Some(volatility) => {
            // Ensure volatility is non-negative
            assert!(
                volatility >= 0.0,
                "Calculated volatility should be non-negative"
            );
            println!("Calculated volatility: {:.6}", volatility);
        }
        None => panic!("Volatility calculation returned None"),
    }
}

#[test]
fn test_interpolation_in_calculate_volatility() {
    // Create an Arc<RwLock<HashMap>> to store the prices with gaps (None values)
    let results_map: Arc<
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
    > = Arc::new(RwLock::new(HashMap::new()));

    // Base timestamp for the data
    let base_timestamp = Utc::now().naive_utc();

    // Add initial defined values
    {
        let mut map = results_map.write().unwrap();
        map.insert(base_timestamp, (Some(100.0), None, None, None, None));
        map.insert(
            base_timestamp + Duration::seconds(1),
            (Some(110.0), None, None, None, None),
        );

        // Introduce gaps with None values that should be interpolated
        map.insert(
            base_timestamp + Duration::seconds(2),
            (None, None, None, None, None),
        );
        map.insert(
            base_timestamp + Duration::seconds(3),
            (None, None, None, None, None),
        );

        // Add more defined values to provide start and end points for interpolation
        map.insert(
            base_timestamp + Duration::seconds(4),
            (Some(130.0), None, None, None, None),
        );
        map.insert(
            base_timestamp + Duration::seconds(5),
            (Some(150.0), None, None, None, None),
        );
    }

    // Set the number of periods for calculation
    let no_of_periods = 6;

    // Call the volatility calculation function
    match calculate_volatility(&results_map, no_of_periods) {
        Some(volatility) => {
            // Ensure volatility is non-negative
            assert!(
                volatility >= 0.0,
                "Calculated volatility should be non-negative"
            );
            println!("Calculated volatility: {:.6}", volatility);

            // Check the interpolated values
            let map = results_map.read().unwrap();
            for (timestamp, entry) in map.iter() {
                let vol = entry.4; // Assume the volatility value is in the fifth element
                println!("Timestamp: {:?}, VOL: {:?}", timestamp, vol);

                // Ensure that the previously None values were interpolated (i.e., not None anymore)
                if timestamp == &(base_timestamp + Duration::seconds(2))
                    || timestamp == &(base_timestamp + Duration::seconds(3))
                {
                    assert!(
                        vol.is_some(),
                        "Interpolated values at timestamp {:?} should not be None",
                        timestamp
                    );
                }
            }
        }
        None => panic!("Volatility calculation returned None"),
    }
}
