/// DO NOT MODIFY THIS FILE
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{thread, time};

lazy_static! {
    static ref FILLS: Vec<Fill> = {
        let mut rdr = csv::Reader::from_path("./trades.csv").expect("Failed to find trades.csv");
        rdr.deserialize()
            .into_iter()
            .filter_map(|result| result.ok())
            .collect()
    };
}

mod date_string {
    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let dt = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Fill {
    #[serde(with = "date_string")]
    pub time: DateTime<Utc>,
    pub direction: i32,
    pub price: Decimal,
    pub quantity: Decimal,
    pub sequence_number: u64,
}

#[must_use]
pub fn get_fills_api(
    start_timestamp_in_seconds: i64,
    end_timestamp_in_seconds: i64,
) -> anyhow::Result<Vec<Fill>> {
    let start_time = DateTime::from_timestamp(start_timestamp_in_seconds, 0)
        .ok_or_else(|| anyhow!("Invalid timestamp"))?;
    let end_time = DateTime::from_timestamp(end_timestamp_in_seconds, 0)
        .ok_or_else(|| anyhow!("Invalid timestamp"))?;

    let interval_length = (end_timestamp_in_seconds - start_timestamp_in_seconds).max(0) as f64;

    // Fetching 1 day's worth of data should take around 1 second
    let sleep_time = time::Duration::from_secs_f64(interval_length * 0.00001);
    thread::sleep(sleep_time);

    Ok(FILLS
        .iter()
        .filter_map(|fill| {
            if fill.time <= start_time || fill.time > end_time {
                None
            } else {
                Some(*fill)
            }
        })
        .collect())
}
