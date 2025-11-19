#[derive(Clone, Copy, Debug)]
pub struct Scientific(pub f64, pub i32);

const LINTHRESH: f64 = 1e-50;
const LOG_LINTHRESH: f64 = -50.0;

impl Scientific {
    pub fn approx_f64(&self) -> f64 {
        self.0 * 10f64.powi(self.1)
    }

    pub fn symlog(&self) -> f64 {
        let mantissa = self.0;
        let exponent = self.1;

        // 1. Handle Zero
        if mantissa == 0.0 {
            return 0.0;
        }

        let sign = mantissa.signum();
        let abs_mantissa = mantissa.abs();

        // 2. Calculate Logs
        // log10(|x|)
        let val_log10 = abs_mantissa.log10() + exponent as f64;

        // 3. Determine which formula to use
        // We compare the magnitude of the value vs the threshold.
        // If the value is more than 16 orders of magnitude larger than the threshold,
        // the "+ 1" in the SymLog formula becomes mathematically irrelevant due to f64 precision limits.
        let magnitude_diff = val_log10 - LOG_LINTHRESH;

        if magnitude_diff > 16.0 {
            // --- HUGE NUMBERS (Log Approximation) ---
            // Formula: log10(|x|) - log10(L)
            // This preserves precision for massive numbers (e.g. 1e100) avoiding overflow.
            sign * magnitude_diff
        } else {
            // --- SMALL / TRANSITION NUMBERS (Exact Math) ---
            // Formula: log10(1 + |x|/L)
            // We need this because near the threshold, the "+ 1" creates the smooth curve.
            // Since magnitude_diff < 16, val_f64 will not overflow f64.

            sign * (1.0 + self.approx_f64().abs() / LINTHRESH).log10()
        }
    }
}

pub fn symlog_formatter(val: f64) -> String {
    if val == 0.0 {
        return "0".to_string();
    } else if (val + LOG_LINTHRESH).abs() < 0.00001 {
        return "1".to_string();
    }

    let sign_str = if val < 0.0 { "-" } else { "" };
    let abs_plot_y = val.abs();

    // INVERSE TRANSFORM
    // |x| = L * (10^|y| - 1)

    // Because we are using a tiny LINTHRESH (1e-20),
    // almost all visible numbers on your plot will be in the "Log" region.
    // In the log region: |x| ~= L * 10^|y|
    // So: log10(|x|) = log10(L) + |y|

    let target_log10 = LOG_LINTHRESH + abs_plot_y;

    // Reconstruct Scientific Notation
    let exponent = target_log10.floor();
    let fractional = target_log10 - exponent;
    let mantissa = 10f64.powf(fractional);

    // Formatting rules
    // If the exponent is very small (e.g. -6), we prefer "1.0e-6" over "0.000001"
    if exponent < -2.0 || exponent > 3.0 {
        format!("{}{:.1}e{:.0}", sign_str, mantissa, exponent)
    } else {
        // For numbers like 0.5, 0.01, 10.0
        let real_val = mantissa * 10f64.powi(exponent as i32);
        // limit decimal places to avoid clutter
        format!("{}{:.4}", sign_str, real_val)
    }
}
