//! Query construction for geocoder searches.
//!
//! Builds a cascading `DisjunctionMaxQuery` that tries progressively
//! less specific matching strategies, similar to Pelias's `FallbackQuery`.

use tantivy::Term;
use tantivy::query::{
    BooleanQuery, BoostQuery, DisjunctionMaxQuery, FuzzyTermQuery, Occur, PhraseQuery, Query,
    TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Schema};

use crate::normalize;
use crate::schema::GeocoderFields;

/// Minimum score threshold for "exact" match quality.
/// Tuned empirically — scores above this indicate high confidence.
pub const EXACT_SCORE_THRESHOLD: f32 = 8.0;

/// Builds the geocoding query for a structured address.
///
/// The query is a `DisjunctionMaxQuery` (takes the max-scoring
/// sub-query) with four levels of specificity:
///
/// 1. **Exact phrase** on street + term city + term state (boost 10.0)
/// 2. **Fuzzy** street (edit distance 1) + term city + term state (boost 5.0)
/// 3. **Fuzzy** street + fuzzy city + term state (boost 2.0)
/// 4. **Phrase** on `full_address` + term state (boost 1.0)
#[must_use]
pub fn build_geocode_query(
    schema: &Schema,
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Box<dyn Query> {
    let norm_street = normalize::normalize(street);
    let norm_city = normalize::normalize(city);
    let norm_state = normalize::normalize_state(state);

    let mut sub_queries: Vec<Box<dyn Query>> = Vec::with_capacity(4);

    // Level 1: Exact phrase match on street + term match on city + state
    if let Some(q) = build_exact_query(schema, fields, &norm_street, &norm_city, &norm_state) {
        sub_queries.push(Box::new(BoostQuery::new(q, 10.0)));
    }

    // Level 2: Fuzzy street + exact city + exact state
    if let Some(q) = build_fuzzy_street_query(fields, &norm_street, &norm_city, &norm_state) {
        sub_queries.push(Box::new(BoostQuery::new(q, 5.0)));
    }

    // Level 3: Fuzzy street + fuzzy city + exact state
    if let Some(q) = build_fuzzy_all_query(fields, &norm_street, &norm_city, &norm_state) {
        sub_queries.push(Box::new(BoostQuery::new(q, 2.0)));
    }

    // Level 4: Full address phrase + state filter
    if let Some(q) = build_full_address_query(fields, &norm_street, &norm_city, &norm_state) {
        sub_queries.push(Box::new(BoostQuery::new(q, 1.0)));
    }

    if sub_queries.is_empty() {
        // Fallback: match anything in the state (shouldn't normally happen)
        return Box::new(TermQuery::new(
            Term::from_field_text(fields.state, &norm_state),
            IndexRecordOption::Basic,
        ));
    }

    Box::new(DisjunctionMaxQuery::new(sub_queries))
}

/// Level 1: Exact phrase on street + term city + term state.
fn build_exact_query(
    schema: &Schema,
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Option<Box<dyn Query>> {
    let street_phrase = build_phrase_query(schema, fields.street, street)?;

    let city_term = TermQuery::new(
        Term::from_field_text(fields.city, &city.to_lowercase()),
        IndexRecordOption::Basic,
    );
    let state_term = TermQuery::new(
        Term::from_field_text(fields.state, state),
        IndexRecordOption::Basic,
    );

    Some(Box::new(BooleanQuery::new(vec![
        (Occur::Must, street_phrase),
        (Occur::Must, Box::new(city_term)),
        (Occur::Must, Box::new(state_term)),
    ])))
}

/// Level 2: Fuzzy terms on street + exact city + exact state.
fn build_fuzzy_street_query(
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Option<Box<dyn Query>> {
    let street_tokens: Vec<&str> = street.split_whitespace().collect();
    if street_tokens.is_empty() {
        return None;
    }

    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    for token in &street_tokens {
        let term = Term::from_field_text(fields.street, &token.to_lowercase());
        let fuzzy = FuzzyTermQuery::new(term, 1, true);
        clauses.push((Occur::Must, Box::new(fuzzy)));
    }

    clauses.push((
        Occur::Must,
        Box::new(TermQuery::new(
            Term::from_field_text(fields.city, &city.to_lowercase()),
            IndexRecordOption::Basic,
        )),
    ));
    clauses.push((
        Occur::Must,
        Box::new(TermQuery::new(
            Term::from_field_text(fields.state, state),
            IndexRecordOption::Basic,
        )),
    ));

    Some(Box::new(BooleanQuery::new(clauses)))
}

/// Level 3: Fuzzy terms on street + fuzzy city + exact state.
fn build_fuzzy_all_query(
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Option<Box<dyn Query>> {
    let street_tokens: Vec<&str> = street.split_whitespace().collect();
    if street_tokens.is_empty() {
        return None;
    }

    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    for token in &street_tokens {
        let term = Term::from_field_text(fields.street, &token.to_lowercase());
        let fuzzy = FuzzyTermQuery::new(term, 1, true);
        clauses.push((Occur::Must, Box::new(fuzzy)));
    }

    // Fuzzy city — each token with edit distance 1
    let city_tokens: Vec<&str> = city.split_whitespace().collect();
    for token in &city_tokens {
        let term = Term::from_field_text(fields.city, &token.to_lowercase());
        let fuzzy = FuzzyTermQuery::new(term, 1, true);
        clauses.push((Occur::Must, Box::new(fuzzy)));
    }

    clauses.push((
        Occur::Must,
        Box::new(TermQuery::new(
            Term::from_field_text(fields.state, state),
            IndexRecordOption::Basic,
        )),
    ));

    Some(Box::new(BooleanQuery::new(clauses)))
}

/// Level 4: Phrase query on the composite `full_address` field + state filter.
fn build_full_address_query(
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Option<Box<dyn Query>> {
    let full = normalize::build_full_address(street, city, state);
    let tokens: Vec<&str> = full.split_whitespace().collect();
    if tokens.len() < 2 {
        return None;
    }

    let phrase = PhraseQuery::new_with_offset(
        tokens
            .iter()
            .map(|t| Term::from_field_text(fields.full_address, &t.to_lowercase()))
            .enumerate()
            .collect(),
    );

    Some(Box::new(BooleanQuery::new(vec![
        (Occur::Must, Box::new(phrase)),
        (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(fields.state, state),
                IndexRecordOption::Basic,
            )),
        ),
    ])))
}

/// Builds a phrase query from a text string against a specific field.
///
/// Returns `None` if the text has fewer than 2 tokens (phrase queries
/// need at least two terms).
fn build_phrase_query(schema: &Schema, field: Field, text: &str) -> Option<Box<dyn Query>> {
    let _ = schema; // reserved for future tokenizer lookups
    let tokens: Vec<&str> = text.split_whitespace().collect();

    if tokens.is_empty() {
        return None;
    }

    if tokens.len() == 1 {
        // Single token: use a regular term query instead of phrase
        return Some(Box::new(TermQuery::new(
            Term::from_field_text(field, &tokens[0].to_lowercase()),
            IndexRecordOption::WithFreqsAndPositions,
        )));
    }

    let terms: Vec<(usize, Term)> = tokens
        .iter()
        .enumerate()
        .map(|(i, t)| (i, Term::from_field_text(field, &t.to_lowercase())))
        .collect();

    Some(Box::new(PhraseQuery::new_with_offset(terms)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::build_schema;

    #[test]
    fn builds_query_for_normal_address() {
        let schema = build_schema();
        let fields = GeocoderFields::from_schema(&schema);
        let query = build_geocode_query(&schema, &fields, "100 N STATE ST", "Chicago", "IL");
        // Verify it doesn't panic and produces a query
        let _debug = format!("{query:?}");
    }

    #[test]
    fn builds_query_for_short_street() {
        let schema = build_schema();
        let fields = GeocoderFields::from_schema(&schema);
        let query = build_geocode_query(&schema, &fields, "BROADWAY", "New York", "NY");
        let _debug = format!("{query:?}");
    }
}
