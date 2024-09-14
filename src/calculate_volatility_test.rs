// src/calculate_volatility_test.rs

use super::calculate_volatility; // Bring the function from the base module
use rand::Rng; // Import random number generation

#[test]
fn test_calculate_volatility_with_random_prices() {
    let mut rng = rand::thread_rng();
    
    // Generate 50 random prices between 50.0 and 150.0
    let vw_values: Vec<f64> = (0..50).map(|_| rng.gen_range(50.0..150.0)).collect();

    // Call the volatility calculation function
    if let Some(volatility) = calculate_volatility(&vw_values) {
        // Ensure volatility is non-negative
        assert!(volatility >= 0.0);
        println!("Calculated volatility: {:.6}", volatility);
    } else {
        panic!("Volatility calculation returned None");
    }
}
