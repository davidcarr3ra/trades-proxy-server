use std::io;
use std::collections::HashSet;
use rust_decimal::Decimal;
use std::time::Instant;
use lru::LruCache;
use std::num::NonZeroUsize;
use log::{info, error, debug, trace};

use crate::server::{get_fills_api, Fill};

pub mod server;

const BUCKET_SIZE: i64 = 3600; // 1 hour in seconds
const PREFETCH_COUNT: i64 = 1; // Prefetch 1 bucket before and after
const MAX_CACHE_SIZE: usize = 200; // Store up to ~8 days of data in memory

fn main() -> anyhow::Result<()> {
    // Initialize the logger
    env_logger::init();
    
    info!("Starting proxy server");
    let start_time = Instant::now();
    
    let mut processor = Processor::new()
        .map_err(|e| anyhow::anyhow!("Failed to initialize processor: {}", e))?;
    
    info!("Processing input queries from stdin");
    for (line_number, query_result) in io::stdin().lines().enumerate() {
        let query = query_result
            .map_err(|e| anyhow::anyhow!("Failed to read line {}: {}", line_number + 1, e))?;
        
        debug!("Processing query [{}]: {}", line_number + 1, query);
        if let Err(e) = processor.process_query(query) {
            error!("Error processing query at line {}: {}", line_number + 1, e);
            // Continue processing other queries even if one fails
        }
    }
    
    let duration = start_time.elapsed();
    info!("Execution completed in {:?}", duration);
    
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

pub struct Processor {
    // LRU Cache to store API results for time buckets
    // Key: (bucket_start_time, bucket_end_time), Value: Vector of fills
    bucket_cache: LruCache<(i64, i64), Vec<Fill>>,
}

impl Processor {
    pub fn new() -> anyhow::Result<Self> {
        let cache_capacity = NonZeroUsize::new(MAX_CACHE_SIZE)
            .ok_or_else(|| anyhow::anyhow!("Cache capacity must be non-zero"))?;
        
        info!("Initializing processor with cache capacity of {}", MAX_CACHE_SIZE);
        Ok(Processor {
            bucket_cache: LruCache::new(cache_capacity),
        })
    }

    // Convert a timestamp to the start of its bucket
    fn get_bucket_start(&self, timestamp: i64) -> i64 {
        // Integer division to get the bucket start time
        (timestamp / BUCKET_SIZE) * BUCKET_SIZE
    }

    // Get fills for a time range, using bucketed cache when possible
    fn get_fills(&mut self, start_time: i64, end_time: i64) -> anyhow::Result<Vec<Fill>> {
        debug!("Getting fills for time range: {} to {}", start_time, end_time);
        
        // Calculate the bucket boundaries for the requested range
        let first_bucket_start = self.get_bucket_start(start_time);
        let last_bucket_start = self.get_bucket_start(end_time);
        
        // First check if any required buckets are missing
        let mut missing_buckets = Vec::new();
        let mut required_missing = false;
        
        // Check required buckets and collect missing ones
        for bucket_start in (first_bucket_start..=last_bucket_start).step_by(BUCKET_SIZE as usize) {
            let bucket_end = bucket_start + BUCKET_SIZE;
            if !self.bucket_cache.contains(&(bucket_start, bucket_end)) {
                missing_buckets.push((bucket_start, bucket_end));
                required_missing = true;
            }
        }
        
        // Only prefetch if we had cache misses in the required range
        if required_missing {
            // Prefetch buckets before and after the required range
            let before = first_bucket_start - PREFETCH_COUNT * BUCKET_SIZE;
            let after = last_bucket_start + PREFETCH_COUNT * BUCKET_SIZE;
            
            // Check prefetch buckets before the required range
            for b in (before..first_bucket_start).step_by(BUCKET_SIZE as usize) {
                if !self.bucket_cache.contains(&(b, b + BUCKET_SIZE)) {
                    missing_buckets.push((b, b + BUCKET_SIZE));
                }
            }
            
            // Check prefetch buckets after the required range
            for b in (after..after + PREFETCH_COUNT * BUCKET_SIZE).step_by(BUCKET_SIZE as usize) {
                if !self.bucket_cache.contains(&(b, b + BUCKET_SIZE)) {
                    missing_buckets.push((b, b + BUCKET_SIZE));
                }
            }
        }
        
        // Fetch any missing buckets
        if !missing_buckets.is_empty() {
            // Find min and max for optimal fetch range
            let min_start = missing_buckets.iter()
                .map(|(s, _)| *s)
                .min()
                .ok_or_else(|| anyhow::anyhow!("Failed to determine minimum bucket start"))?;
                
            let max_end = missing_buckets.iter()
                .map(|(_, e)| *e)
                .max()
                .ok_or_else(|| anyhow::anyhow!("Failed to determine maximum bucket end"))?;
            
            debug!("Fetching {} missing buckets [{} to {}]", missing_buckets.len(), min_start, max_end);
            
            // Make a single API call to fetch all missing data
            let all_fills = get_fills_api(min_start, max_end)
                .map_err(|e| anyhow::anyhow!("API call failed: {}", e))?;
            
            // Distribute the fetched data into the appropriate buckets
            for (bucket_start, bucket_end) in missing_buckets {
                // Filter fills that belong to this bucket
                let bucket_fills: Vec<Fill> = all_fills.iter()
                    .filter(|fill| {
                        let fill_timestamp = fill.time.timestamp();
                        fill_timestamp > bucket_start && fill_timestamp <= bucket_end
                    })
                    .copied()
                    .collect();
                
                trace!("Storing {} fills in bucket [{}, {}]", bucket_fills.len(), bucket_start, bucket_end);
                
                // Store in cache
                self.bucket_cache.put((bucket_start, bucket_end), bucket_fills);
                
                // Log prefetch information for buckets outside the requested range
                if bucket_start < first_bucket_start || bucket_start > last_bucket_start {
                    debug!("Prefetched bucket: [{}, {}]", bucket_start, bucket_end);
                }
            }
        } else {
            debug!("All required buckets found in cache");
        }
        
        // Combine fills from all relevant buckets and filter by the exact time range
        let mut result = Vec::new();
        for bucket_start in (first_bucket_start..=last_bucket_start).step_by(BUCKET_SIZE as usize) {
            let bucket_end = bucket_start + BUCKET_SIZE;
            // Get from cache, which automatically updates LRU order
            if let Some(bucket_fills) = self.bucket_cache.get(&(bucket_start, bucket_end)) {
                trace!("Processing {} fills from bucket [{}, {}]", bucket_fills.len(), bucket_start, bucket_end);
                
                for fill in bucket_fills {
                    // Convert DateTime to timestamp for comparison
                    let fill_timestamp = fill.time.timestamp();
                    
                    // Only include fills within the requested time range: (> start, <= end)
                    if fill_timestamp > start_time && fill_timestamp <= end_time {
                        result.push(*fill);
                    }
                }
            } else {
                // This should never happen as we just filled the cache, but handle it just in case
                return Err(anyhow::anyhow!(
                    "Cache inconsistency: bucket [{}, {}] not found after prefetching", 
                    bucket_start, bucket_end
                ));
            }
        }
        
        info!("Returning {} fills for time range {} to {}", result.len(), start_time, end_time);
        Ok(result)
    }

    pub fn process_query(&mut self, query: String) -> anyhow::Result<()> {
        let parts: Vec<&str> = query.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(anyhow::anyhow!("Invalid query format: expected 3 parts"));
        }

        let query_type = parts[0];
        let start_time = parts[1].parse::<i64>()
            .map_err(|e| anyhow::anyhow!("Invalid start time: {}", e))?;
        let end_time = parts[2].parse::<i64>()
            .map_err(|e| anyhow::anyhow!("Invalid end time: {}", e))?;

        // Ensure the time constraint is met
        if end_time - start_time > 3600 {
            return Err(anyhow::anyhow!("Time range exceeds 3600 seconds"));
        }

        debug!("Processing query type '{}' for time range {} to {}", query_type, start_time, end_time);
        
        match self.get_fills(start_time, end_time) {
            Ok(fills) => {
                match query_type {
                    "C" => {
                        // Count unique taker trades by their sequence numbers
                        let unique_sequence_numbers: HashSet<u64> = fills.iter()
                            .map(|fill| fill.sequence_number)
                            .collect();
                        println!("{}", unique_sequence_numbers.len());
                    },
                    "B" => {
                        // Count unique sequence numbers for market buys (direction = 1)
                        // A market buy is a taker trade with direction = 1
                        let buy_sequences: HashSet<u64> = fills.iter()
                            .filter(|fill| fill.direction == 1)
                            .map(|fill| fill.sequence_number)
                            .collect();
                        println!("{}", buy_sequences.len());
                    },
                    "S" => {
                        // Count unique sequence numbers for market sells (direction = -1)
                        // A market sell is a taker trade with direction = -1
                        let sell_sequences: HashSet<u64> = fills.iter()
                            .filter(|fill| fill.direction == -1)
                            .map(|fill| fill.sequence_number)
                            .collect();
                        println!("{}", sell_sequences.len());
                    },
                    "V" => {
                        // Total trading volume in USD
                        let total_volume = fills.iter()
                            .fold(Decimal::ZERO, |acc, fill| {
                                acc + (fill.price * fill.quantity)
                            });
                        println!("{}", total_volume);
                    },
                    _ => return Err(anyhow::anyhow!("Invalid query type: {}", query_type)),
                }
                Ok(())
            },
            Err(e) => Err(anyhow::anyhow!("Failed to fetch fills: {}", e)),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    // Helper function to create a test fill
    fn create_test_fill(
        sequence_number: u64,
        timestamp: i64,
        direction: i32,
        price: Decimal,
        quantity: Decimal,
    ) -> Fill {
        Fill {
            sequence_number,
            time: Utc.timestamp_opt(timestamp, 0).unwrap(),
            direction,
            price,
            quantity,
        }
    }

    #[test]
    fn test_processor_initialization() {
        // Test that we can create a new processor
        let processor = Processor::new();
        assert!(processor.is_ok());
    }

    #[test]
    fn test_get_bucket_start() {
        // Test the bucket calculation logic
        let processor = Processor::new().unwrap();
        
        // Test with a timestamp at the bucket boundary
        assert_eq!(processor.get_bucket_start(3600), 3600);
        
        // Test with a timestamp in the middle of a bucket
        assert_eq!(processor.get_bucket_start(4500), 3600);
    }

    #[test]
    fn test_query_calculations() {
        // Create a small set of test fills
        let fills = vec![
            // Two buys with the same sequence number (should count as 1)
            create_test_fill(1, 1701386700, 1, dec!(100.0), dec!(1.5)),
            create_test_fill(1, 1701386750, 1, dec!(101.0), dec!(0.5)),
            
            // One sell
            create_test_fill(2, 1701386800, -1, dec!(102.0), dec!(2.0)),
        ];
        
        // Test count (C) - should count unique sequence numbers
        let unique_sequences: HashSet<u64> = fills.iter()
            .map(|fill| fill.sequence_number)
            .collect();
        assert_eq!(unique_sequences.len(), 2);
        
        // Test buys (B) - should count unique buy sequence numbers
        let buy_sequences: HashSet<u64> = fills.iter()
            .filter(|fill| fill.direction == 1)
            .map(|fill| fill.sequence_number)
            .collect();
        assert_eq!(buy_sequences.len(), 1);
        
        // Test sells (S) - should count unique sell sequence numbers
        let sell_sequences: HashSet<u64> = fills.iter()
            .filter(|fill| fill.direction == -1)
            .map(|fill| fill.sequence_number)
            .collect();
        assert_eq!(sell_sequences.len(), 1);
        
        // Test volume (V) - should calculate total trading volume
        let total_volume = fills.iter()
            .fold(Decimal::ZERO, |acc, fill| {
                acc + (fill.price * fill.quantity)
            });
        // (100.0 * 1.5) + (101.0 * 0.5) + (102.0 * 2.0) = 150 + 50.5 + 204 = 404.5
        assert_eq!(total_volume, dec!(404.5));
    }

    #[test]
    fn test_invalid_query_format() {
        // Test that invalid queries are properly rejected
        let mut processor = Processor::new().unwrap();
        
        // Test with invalid query format
        let result = processor.process_query("invalid query".to_string());
        assert!(result.is_err());
    }
}
