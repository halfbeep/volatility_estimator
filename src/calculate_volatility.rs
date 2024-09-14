// src/calculate_volatility.rs

pub fn calculate_volatility(vw_values: &[f64]) -> Option<f64> {
    let n = vw_values.len();
    if n == 0 {
        return None;
    }

    let mean = vw_values.iter().sum::<f64>() / n as f64;
    let variance = vw_values
        .iter()
        .map(|&x| (x - mean).powi(2))
        .sum::<f64>() / n as f64;
    let standard_deviation = variance.sqrt();
    
    Some(standard_deviation)
}