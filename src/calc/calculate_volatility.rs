use chrono::NaiveDateTime;
use std::collections::HashMap;

#[path = "../util/rounding.rs"]
mod rounding;
use rounding::round_to_period;

pub fn calculate_volatility(
    results_map: &mut HashMap<
        NaiveDateTime,
        (
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
            Option<f64>,
        ),
    >,
    no_of_periods: usize,
    timespan: &str,
) -> Option<f64> {
    // Round the timestamps to the specified `timespan` using round_to_period first
    let mut rounded_map: HashMap<NaiveDateTime, _> = HashMap::new();
    for (timestamp, value) in results_map.drain() {
        let rounded_timestamp = round_to_period(timestamp, timespan);
        rounded_map.insert(rounded_timestamp, value);
    }

    // Replace the old results_map with the rounded_map
    *results_map = rounded_map;

    // Sort on timestamps
    let mut timestamps: Vec<NaiveDateTime> = results_map.keys().cloned().collect();
    timestamps.sort(); // Sort by timestamp (ascending)

    // Keep most recent `no_of_periods` timestamps
    if timestamps.len() > no_of_periods {
        for old_timestamp in &timestamps[..timestamps.len() - no_of_periods] {
            results_map.remove(old_timestamp); // Remove older timestamps
        }
    }

    // Calculate `vol` for each entry as the highest value among VW, AP, KR, and CA
    for (_timestamp, (vw, ap, kr, ca, vol)) in results_map.iter_mut() {
        // Collect non-None values into a vector
        let mut values = vec![];
        if let Some(vw_value) = *vw {
            // Volume weighted
            values.push(vw_value);
        }
        if let Some(ap_value) = *ap {
            // Average in timespan
            values.push(ap_value);
        }
        if let Some(kr_value) = *kr {
            // Kraken price is avg of ohlc
            values.push(kr_value);
        }
        if let Some(ca_value) = *ca {
            // CoinAPI price is avg of ohlc
            values.push(ca_value);
        }

        // Calculate the maximum value or set vol to None if the vector is empty
        *vol = if values.is_empty() {
            None
        } else {
            Some(values.into_iter().fold(f64::MIN, f64::max))
        };
    }

    // Update the `vol` values back to the `results_map`, converting None to NaN for proper interpolation
    let mut vol_values: Vec<(NaiveDateTime, f64)> = results_map
        .iter()
        .map(|(timestamp, (_, _, _, _, vol))| (*timestamp, vol.unwrap_or(f64::NAN)))
        .collect();

    // Ensure the values are sorted by timestamps for interpolation
    vol_values.sort_by_key(|&(timestamp, _)| timestamp);

    // Perform linear interpolation on None segments (now represented by NaN)
    let mut i = 0;

    // Backward Interpolation
    // Handle leading NaN values by finding the first non-NaN value
    if !vol_values.is_empty() && vol_values[0].1.is_nan() {
        // Find the first non-NaN value
        let mut first_valid_value = None;
        for &(_, value) in &vol_values {
            if !value.is_nan() {
                first_valid_value = Some(value);
                break;
            }
        }

        // If a valid value is found, set it for all leading NaNs
        if let Some(valid_value) = first_valid_value {
            for &mut (_, ref mut value) in vol_values.iter_mut() {
                if value.is_nan() {
                    *value = valid_value;
                } else {
                    break;
                }
            }
        } else {
            // If all values are NaN, use 0.0 as a fallback
            for &mut (_, ref mut value) in vol_values.iter_mut() {
                if value.is_nan() {
                    *value = 0.0;
                }
            }
        }
    }

    // Interpolation between values
    // Now handle the remaining NaN values
    while i < vol_values.len() {
        // Find the start of a None (NaN) segment
        if vol_values[i].1.is_nan() {
            let start_index = i;
            let start_value = if start_index > 0 {
                vol_values[start_index - 1].1
            } else {
                // If there is no valid start value, assume 0.0 as a fallback
                0.0
            };

            // Find the end of the None (NaN) segment
            while i < vol_values.len() && vol_values[i].1.is_nan() {
                i += 1;
            }

            // Determine the end value for interpolation
            let end_value = if i < vol_values.len() {
                vol_values[i].1
            } else {
                // Interpolation Projection
                // If at the end of the dataset, continue linear progression from the last known value
                start_value
            };

            let end_index = i;

            // Fill the values linearly between start_index and end_index
            let num_steps = (end_index - start_index) as f64;
            for j in 0..(end_index - start_index) {
                vol_values[start_index + j].1 =
                    start_value + (end_value - start_value) * (j as f64 + 1.0) / (num_steps + 1.0);
            }
        } else {
            i += 1;
        }
    }

    // Update the `vol` values back to the `results_map`
    for (timestamp, vol_value) in &vol_values {
        if let Some(entry) = results_map.get_mut(timestamp) {
            entry.4 = Some(*vol_value); // Update the fifth element (vol)
        }
    }

    // Extract all `vol` values into a vector, again filtering out `None` values !!
    // TODO: this filter is belt & braces and should probably be removed
    let vol_values: Vec<f64> = results_map
        .values()
        .filter_map(|(_, _, _, _, vol)| *vol)
        .collect();
    let n = vol_values.len();
    if n == 0 {
        return None;
    }

    // Calculate mean
    let mean = vol_values.iter().sum::<f64>() / n as f64;

    // Calculate variance
    let variance = vol_values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;

    // Calculate standard deviation
    let standard_deviation = variance.sqrt();

    Some(standard_deviation)
}
