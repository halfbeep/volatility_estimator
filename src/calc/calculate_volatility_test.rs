use super::calculate_volatility::calculate_volatility; // Bring the function from the base module
use chrono::{NaiveDateTime, Utc};
use rand::Rng; // Import random number generation
use std::collections::HashMap;

#[test]
fn test_calculate_volatility_with_random_prices() {
    let mut rng = rand::thread_rng();

    // Create a mutable HashMap to store the random prices
    let mut results_map: HashMap<
        NaiveDateTime,
        (
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
        ),
    > = HashMap::new();

    // Generate 50 random prices between 50.0 and 150.0 with the current time as timestamps
    for i in 0..50 {
        let timestamp = Utc::now().naive_utc() - chrono::Duration::seconds(i);
        let random_price = rng.gen_range(50.0..150.0);

        // Insert the random price into the HashMap. Let's assume it corresponds to the `vw` field.
        results_map.insert(timestamp, (Some(random_price), None, None, None, None));
    }

    // Set the number of periods and the timespan for the volatility calculation
    let no_of_periods = 50;
    let timespan = "second";

    // Call the volatility calculation function
    if let Some(volatility) = calculate_volatility(&mut results_map, no_of_periods, timespan) {
        // Ensure volatility is non-negative
        assert!(volatility >= 0.0);
        println!("Calculated volatility: {:.6}", volatility);
    } else {
        panic!("Volatility calculation returned None");
    }
}
