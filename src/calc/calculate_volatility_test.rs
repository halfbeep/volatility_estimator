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

#[test]
fn test_calculate_volatility_with_given_returns() {
    // The provided array of returns
    let returns = [
        -0.009103197429499166,
        0.00936993735757305,
        0.029206122417959775,
        0.02454310503968983,
        -0.003897404481121453,
        -0.0016206733055013926,
        -0.059715779339231896,
        -0.032363131598655955,
        0.03308966289047178,
        -0.02687462172174898,
        -0.0005457007004976708,
        -0.020294105989139413,
        0.04498320993648113,
        -0.03311026236166164,
        -0.02686631709822145,
        -0.006869607080694805,
        -0.046841730530681004,
        -0.004572433598902075,
        0.0033733624454148074,
        0.017310600702869182,
        0.0146524064171123,
        -0.007494466111520875,
        0.0017523550589957602,
        0.021652771298927145,
        0.006357238130444961,
        -0.01630196872976134,
        -0.03313538724334515,
        0.014149029131476348,
        0.009171659928307856,
    ];

    // Calculate the mean of the returns
    let n = returns.len();
    let mean = returns.iter().sum::<f64>() / n as f64;

    // Calculate the variance of the returns
    let variance = returns.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;

    // Calculate the standard deviation (volatility)
    let calculated_volatility = variance.sqrt();

    // Expected volatility
    let expected_volatility = 0.024631;

    // Assert the calculated volatility is approximately equal to the expected volatility
    assert!(
        (calculated_volatility - expected_volatility).abs() < 1e-6,
        "Calculated volatility: {:.6}, Expected volatility: {:.6}",
        calculated_volatility,
        expected_volatility
    );
}
