//! Concrete crime data source implementations.
//!
//! Each module implements the [`CrimeSource`](crate::CrimeSource) trait for a
//! specific data provider.

pub mod boston;
pub mod chicago;
pub mod dc;
pub mod denver;
pub mod la;
pub mod nyc;
pub mod philly;
pub mod seattle;
pub mod sf;
