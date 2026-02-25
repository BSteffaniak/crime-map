//! Tantivy index schema definition for the geocoder.
//!
//! Defines the field layout used for US address search. The schema is
//! designed for structured geocoding queries where street, city, and
//! state are known components.

use tantivy::Index;
use tantivy::schema::{
    self, Field, NumericOptions, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::TextAnalyzer;

/// Names of all fields in the geocoder schema.
pub struct FieldNames;

impl FieldNames {
    /// Normalized street address (e.g., "100 NORTH STATE STREET").
    pub const STREET: &'static str = "street";
    /// Normalized city name (e.g., "CHICAGO").
    pub const CITY: &'static str = "city";
    /// Two-letter state code, exact match (e.g., "IL").
    pub const STATE: &'static str = "state";
    /// ZIP/postal code, exact match.
    pub const POSTCODE: &'static str = "postcode";
    /// Latitude (WGS84).
    pub const LAT: &'static str = "lat";
    /// Longitude (WGS84).
    pub const LON: &'static str = "lon";
    /// Data source tag ("oa" or "osm").
    pub const SOURCE: &'static str = "source";
    /// Composite full address for fallback matching.
    pub const FULL_ADDRESS: &'static str = "full_address";
}

/// All resolved field handles for efficient document construction.
#[derive(Debug, Clone)]
pub struct GeocoderFields {
    /// Normalized street address.
    pub street: Field,
    /// City name.
    pub city: Field,
    /// State code (exact match).
    pub state: Field,
    /// Postal code (exact match).
    pub postcode: Field,
    /// Latitude.
    pub lat: Field,
    /// Longitude.
    pub lon: Field,
    /// Source tag.
    pub source: Field,
    /// Composite full address.
    pub full_address: Field,
}

impl GeocoderFields {
    /// Resolves field handles from a schema.
    ///
    /// # Panics
    ///
    /// Panics if the schema does not contain the expected fields
    /// (should only happen if the schema was not built by [`build_schema`]).
    #[must_use]
    pub fn from_schema(schema: &Schema) -> Self {
        Self {
            street: schema
                .get_field(FieldNames::STREET)
                .expect("schema missing street field"),
            city: schema
                .get_field(FieldNames::CITY)
                .expect("schema missing city field"),
            state: schema
                .get_field(FieldNames::STATE)
                .expect("schema missing state field"),
            postcode: schema
                .get_field(FieldNames::POSTCODE)
                .expect("schema missing postcode field"),
            lat: schema
                .get_field(FieldNames::LAT)
                .expect("schema missing lat field"),
            lon: schema
                .get_field(FieldNames::LON)
                .expect("schema missing lon field"),
            source: schema
                .get_field(FieldNames::SOURCE)
                .expect("schema missing source field"),
            full_address: schema
                .get_field(FieldNames::FULL_ADDRESS)
                .expect("schema missing full_address field"),
        }
    }
}

/// Builds the Tantivy schema for the geocoder index.
///
/// Fields:
/// - `street` — TEXT (tokenized + stored): normalized street address
/// - `city` — TEXT (tokenized + stored): city name
/// - `state` — STRING (exact match + stored): two-letter state code
/// - `postcode` — STRING (exact match + stored): ZIP code
/// - `lat` — f64 (stored + fast): latitude
/// - `lon` — f64 (stored + fast): longitude
/// - `source` — STRING (stored): data source tag
/// - `full_address` — TEXT (tokenized, not stored): composite search field
#[must_use]
pub fn build_schema() -> Schema {
    let mut builder = Schema::builder();

    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_index_option(schema::IndexRecordOption::WithFreqsAndPositions);

    let text_stored = TextOptions::default()
        .set_indexing_options(text_field_indexing.clone())
        .set_stored();

    let text_indexed_only = TextOptions::default().set_indexing_options(text_field_indexing);

    // Tokenized + stored text fields
    builder.add_text_field(FieldNames::STREET, text_stored.clone());
    builder.add_text_field(FieldNames::CITY, text_stored);

    // Exact-match string fields
    builder.add_text_field(FieldNames::STATE, STRING | STORED);
    builder.add_text_field(FieldNames::POSTCODE, STRING | STORED);
    builder.add_text_field(FieldNames::SOURCE, STRING | STORED);

    // Numeric fields — stored and indexed as fast fields for retrieval
    let f64_options = NumericOptions::default().set_stored().set_fast();
    builder.add_f64_field(FieldNames::LAT, f64_options.clone());
    builder.add_f64_field(FieldNames::LON, f64_options);

    // Composite text field for fallback searches (not stored)
    builder.add_text_field(FieldNames::FULL_ADDRESS, text_indexed_only);

    builder.build()
}

/// Registers custom tokenizers on the given index.
///
/// Currently uses Tantivy's built-in default tokenizer (lowercase +
/// split on non-alphanumeric). Normalization/synonym expansion is done
/// before indexing and querying rather than through custom tokenizers,
/// which keeps the pipeline simpler and fully deterministic.
pub fn register_tokenizers(index: &Index) {
    // The default tokenizer is already registered. We use a simple
    // lowercase tokenizer which matches our pre-normalized input well.
    // If we ever need edge n-grams (for autocomplete), we would
    // register a custom tokenizer here.
    index.tokenizers().register(
        "default",
        TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_fields() {
        let schema = build_schema();
        assert!(schema.get_field(FieldNames::STREET).is_ok());
        assert!(schema.get_field(FieldNames::CITY).is_ok());
        assert!(schema.get_field(FieldNames::STATE).is_ok());
        assert!(schema.get_field(FieldNames::POSTCODE).is_ok());
        assert!(schema.get_field(FieldNames::LAT).is_ok());
        assert!(schema.get_field(FieldNames::LON).is_ok());
        assert!(schema.get_field(FieldNames::SOURCE).is_ok());
        assert!(schema.get_field(FieldNames::FULL_ADDRESS).is_ok());
    }

    #[test]
    fn fields_resolve_from_schema() {
        let schema = build_schema();
        let fields = GeocoderFields::from_schema(&schema);
        // Ensure the fields are distinct
        assert_ne!(fields.street, fields.city);
        assert_ne!(fields.lat, fields.lon);
    }
}
