use chrono::NaiveDateTime;
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub fn calculate_volatility(
    results_map: &Arc<
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
    >,
    no_of_periods: usize,
) -> Option<f64> {
    // Lock the map here
    let mut results_map = results_map.write().unwrap();

    let mut timestamps: Vec<NaiveDateTime> = results_map.keys().cloned().collect();
    timestamps.sort(); // Sort by timestamp (ascending)

    // Keep most recent `no_of_periods` timestamps
    if timestamps.len() > no_of_periods {
        for old_timestamp in &timestamps[..timestamps.len() - no_of_periods] {
            results_map.remove(old_timestamp); // Remove older timestamps
        }
    }

    // Calculate `vol price` for each entry as the highest value among VW, AP, KR, and CA
    for (_timestamp, (vw, ap, kr, ca, vol)) in results_map.iter_mut() {
        // Collect non-None values into a vector
        let mut values = vec![];
        if let Some(vw_value) = *vw {
            values.push(vw_value);
        }
        if let Some(ap_value) = *ap {
            values.push(ap_value);
        }
        if let Some(kr_value) = *kr {
            values.push(kr_value);
        }
        if let Some(ca_value) = *ca {
            values.push(ca_value);
        }

        // Calculate the maximum value or set vol to None if the vector is empty
        *vol = if values.is_empty() {
            None
        } else {
            Some(values.into_iter().fold(f64::MAX, f64::min))
        };
    }

    // Interpolate missing values (None) as before
    let mut vol_values: Vec<(NaiveDateTime, f64)> = results_map
        .iter()
        .map(|(timestamp, (_, _, _, _, vol))| (*timestamp, vol.unwrap_or(f64::NAN)))
        .collect();

    // Ensure the values are sorted by timestamp for interpolation
    vol_values.sort_by_key(|&(timestamp, _)| timestamp);

    // Perform linear interpolation on None segments (now represented by NaN)
    let mut i = 0;

    // Backward Interpolation
    if !vol_values.is_empty() && vol_values[0].1.is_nan() {
        let mut first_valid_value = None;
        for &(_, value) in &vol_values {
            if !value.is_nan() {
                first_valid_value = Some(value);
                break;
            }
        }
        if let Some(valid_value) = first_valid_value {
            for &mut (_, ref mut value) in vol_values.iter_mut() {
                if value.is_nan() {
                    *value = valid_value;
                } else {
                    break;
                }
            }
        } else {
            for &mut (_, ref mut value) in vol_values.iter_mut() {
                if value.is_nan() {
                    *value = 0.0;
                }
            }
        }
    }

    // Interpolation between values
    while i < vol_values.len() {
        if vol_values[i].1.is_nan() {
            let start_index = i;
            let start_value = if start_index > 0 {
                vol_values[start_index - 1].1
            } else {
                0.0
            };
            while i < vol_values.len() && vol_values[i].1.is_nan() {
                i += 1;
            }
            let end_value = if i < vol_values.len() {
                vol_values[i].1
            } else {
                start_value
            };
            let end_index = i;
            let num_steps = (end_index - start_index) as f64;
            for j in 0..(end_index - start_index) {
                vol_values[start_index + j].1 =
                    start_value + (end_value - start_value) * (j as f64 + 1.0) / (num_steps + 1.0);
            }
        } else {
            i += 1;
        }
    }

    // Update the `volp` values back to the `results_map`
    for (timestamp, vol_value) in &vol_values {
        if let Some(entry) = results_map.get_mut(timestamp) {
            entry.4 = Some(*vol_value); // Update the fifth element (vol)
        }
    }

    // Calculate returns: (current_vol - previous_vol) / previous_vol
    let mut returns = vec![];
    for i in 1..vol_values.len() {
        let current_vol = vol_values[i].1;
        let previous_vol = vol_values[i - 1].1;
        let return_value = (current_vol - previous_vol) / previous_vol;
        debug!(
            "Price {} Previous {} Return {}",
            current_vol, previous_vol, return_value
        );
        returns.push(return_value);
    }

    let n = returns.len();
    if n == 0 {
        return None;
    }

    // Calculate mean of returns
    let mean_return = returns.iter().sum::<f64>() / n as f64;

    // Calculate variance of returns
    let variance = returns
        .iter()
        .map(|&x| (x - mean_return).powi(2))
        .sum::<f64>()
        / (n - 1) as f64;

    // Calculate standard deviation of returns
    let standard_deviation = variance.sqrt();

    Some(standard_deviation)
}
