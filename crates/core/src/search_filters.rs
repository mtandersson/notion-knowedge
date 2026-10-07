//! Typed narrowing of persisted Notion metadata. Missing properties never match.
use crate::indexed::PropertyValue;
use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::{Deserialize, Deserializer, Serialize};
fn present<'de, T: Deserialize<'de>, D: Deserializer<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataFilters {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub workspace_ids: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub database_ids: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub data_source_ids: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub page_kind: Option<PageKind>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub edited: Option<DateRange>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub properties: Option<Vec<PropertyFilter>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageKind {
    Standalone,
    Database,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DateRange {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub from: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub until: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operator", rename_all = "snake_case", deny_unknown_fields)]
pub enum PropertyFilter {
    Equals {
        property_id: String,
        value: PropertyValue,
    },
    Contains {
        property_id: String,
        value: String,
    },
    Date {
        property_id: String,
        range: DateRange,
    },
}
fn instant(value: &str) -> Result<DateTime<FixedOffset>, &'static str> {
    if value.len() > 128 {
        return Err("date timestamp exceeds 128 bytes");
    }
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| "date bounds require RFC3339 timestamps with an offset")
}
impl DateRange {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.from.is_none() && self.until.is_none() {
            return Err("date range requires from or until");
        }
        let from = self.from.as_deref().map(instant).transpose()?;
        let until = self.until.as_deref().map(instant).transpose()?;
        if matches!((from, until), (Some(a), Some(b)) if a >= b) {
            return Err("date range from must precede until");
        }
        Ok(())
    }
    /// Half-open range. RFC3339 offsets/fractions represent actual instants.
    pub fn matches(&self, value: &str) -> Result<bool, &'static str> {
        self.validate()?;
        let value = instant(value)?;
        Ok(self
            .from
            .as_deref()
            .map(instant)
            .transpose()?
            .is_none_or(|a| value >= a)
            && self
                .until
                .as_deref()
                .map(instant)
                .transpose()?
                .is_none_or(|b| value < b))
    }
}
fn valid_property_date(value: &str) -> bool {
    if value.len() == 10 {
        NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
    } else {
        instant(value).is_ok()
    }
}

fn bounded(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.chars().count() <= max && !value.chars().any(char::is_control)
}
impl MetadataFilters {
    pub fn validate(&self) -> Result<(), &'static str> {
        for ids in [
            &self.workspace_ids,
            &self.database_ids,
            &self.data_source_ids,
        ]
        .into_iter()
        .flatten()
        {
            if ids.is_empty() || ids.len() > 100 || ids.iter().any(|v| !bounded(v, 128)) {
                return Err(
                    "source filters require 1 to 100 nonempty IDs of at most 128 characters",
                );
            }
        }
        if let Some(range) = &self.edited {
            range.validate()?;
        }
        if let Some(properties) = &self.properties {
            if properties.is_empty() || properties.len() > 20 {
                return Err("properties require 1 to 20 typed predicates");
            }
            for p in properties {
                let id = match p {
                    PropertyFilter::Equals { property_id, value } => {
                        let valid = match value {
                            PropertyValue::Number(n) => n.is_finite(),
                            PropertyValue::Text(v) => v.chars().count() <= 4096,
                            PropertyValue::Strings(v)
                            | PropertyValue::PageIds(v)
                            | PropertyValue::PersonIds(v) => {
                                v.len() <= 100 && v.iter().all(|v| bounded(v, 4096))
                            }
                            PropertyValue::Date { start, end } => {
                                valid_property_date(start)
                                    && end.as_ref().is_none_or(|v| valid_property_date(v))
                            }
                            _ => true,
                        };
                        if !valid {
                            return Err("invalid typed property value");
                        }
                        property_id
                    }
                    PropertyFilter::Contains { property_id, value } => {
                        if !bounded(value, 4096) {
                            return Err(
                                "contains requires a nonempty string of at most 4096 characters",
                            );
                        }
                        property_id
                    }
                    PropertyFilter::Date { property_id, range } => {
                        range.validate()?;
                        property_id
                    }
                };
                if !bounded(id, 128) {
                    return Err("property IDs require 1 to 128 characters");
                }
            }
        }
        Ok(())
    }
    /// Reference evaluator for adapters whose bounded scan already has canonical metadata.
    pub fn matches_metadata(
        &self,
        metadata: &crate::indexed::IndexedMetadata,
    ) -> Result<bool, &'static str> {
        self.validate()?;
        let source = &metadata.source;
        if self
            .workspace_ids
            .as_ref()
            .is_some_and(|ids| !ids.contains(&source.workspace_id))
            || self.database_ids.as_ref().is_some_and(|ids| {
                source
                    .database_id
                    .as_ref()
                    .is_none_or(|id| !ids.contains(id))
            })
            || self.data_source_ids.as_ref().is_some_and(|ids| {
                source
                    .data_source_id
                    .as_ref()
                    .is_none_or(|id| !ids.contains(id))
            })
            || self.page_kind.is_some_and(|kind| {
                (kind == PageKind::Database)
                    != (source.database_id.is_some() || source.data_source_id.is_some())
            })
        {
            return Ok(false);
        }
        self.matches_values(&metadata.last_edited_time, &metadata.properties)
    }

    pub fn matches_values(
        &self,
        edited: &str,
        properties: &BTreeMap<String, PropertyValue>,
    ) -> Result<bool, &'static str> {
        if let Some(range) = &self.edited
            && !range.matches(edited)?
        {
            return Ok(false);
        }
        for predicate in self.properties.iter().flatten() {
            let matches = match predicate {
                PropertyFilter::Equals { property_id, value } => {
                    properties.get(property_id) == Some(value)
                }
                PropertyFilter::Contains { property_id, value } => {
                    matches!(properties.get(property_id), Some(PropertyValue::Strings(values) | PropertyValue::PageIds(values) | PropertyValue::PersonIds(values)) if values.contains(value))
                }
                PropertyFilter::Date { property_id, range } => match properties.get(property_id) {
                    // Notion date-only values use midnight UTC for range comparison.
                    Some(PropertyValue::Date { start, .. }) => {
                        let start = if start.len() == 10 {
                            format!("{start}T00:00:00Z")
                        } else {
                            start.clone()
                        };
                        range.matches(&start)?
                    }
                    _ => false,
                },
            };
            if !matches {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dates_compare_instants_with_half_open_bounds() {
        let range = DateRange {
            from: Some("2026-10-07T10:00:00.100Z".into()),
            until: Some("2026-10-07T12:00:00+01:00".into()),
        };
        assert!(range.matches("2026-10-07T12:00:00.100+02:00").unwrap());
        assert!(!range.matches("2026-10-07T10:00:00.099999Z").unwrap());
        assert!(!range.matches("2026-10-07T11:00:00Z").unwrap());
        assert!(range.matches("invalid").is_err());
        for range in [
            DateRange {
                from: None,
                until: None,
            },
            DateRange {
                from: Some("2026-10-07".into()),
                until: None,
            },
            DateRange {
                from: Some("2026-10-07T10:00:00Z".into()),
                until: Some("2026-10-07T11:00:00+01:00".into()),
            },
        ] {
            assert!(range.validate().is_err());
        }
    }
    #[test]
    fn property_filters_are_typed_conjunctions_and_distinguish_absent_and_null() {
        let properties = BTreeMap::from([
            (
                "tags-id".into(),
                PropertyValue::Strings(vec!["rust".into(), "pågående".into()]),
            ),
            ("flag-id".into(), PropertyValue::Boolean(true)),
            ("null-id".into(), PropertyValue::Null),
            (
                "date-id".into(),
                PropertyValue::Date {
                    start: "2026-10-07".into(),
                    end: None,
                },
            ),
        ]);
        let mut filters = MetadataFilters {
            properties: Some(vec![
                PropertyFilter::Contains {
                    property_id: "tags-id".into(),
                    value: "rust".into(),
                },
                PropertyFilter::Equals {
                    property_id: "flag-id".into(),
                    value: PropertyValue::Boolean(true),
                },
                PropertyFilter::Date {
                    property_id: "date-id".into(),
                    range: DateRange {
                        from: Some("2026-10-07T00:00:00Z".into()),
                        until: Some("2026-10-08T00:00:00Z".into()),
                    },
                },
                PropertyFilter::Equals {
                    property_id: "null-id".into(),
                    value: PropertyValue::Null,
                },
            ]),
            ..Default::default()
        };
        filters.validate().unwrap();
        assert!(filters.matches_values("not needed", &properties).unwrap());
        filters
            .properties
            .as_mut()
            .unwrap()
            .push(PropertyFilter::Equals {
                property_id: "missing".into(),
                value: PropertyValue::Null,
            });
        assert!(!filters.matches_values("not needed", &properties).unwrap());
        filters.properties = Some(vec![PropertyFilter::Equals {
            property_id: "flag-id".into(),
            value: PropertyValue::Text("true".into()),
        }]);
        assert!(!filters.matches_values("", &properties).unwrap());
    }
    #[test]
    fn typed_values_round_trip_and_date_equality_preserves_original_representation() {
        let default = MetadataFilters::default();
        assert_eq!(
            serde_json::from_value::<MetadataFilters>(serde_json::to_value(&default).unwrap())
                .unwrap(),
            default
        );
        let properties = BTreeMap::from([(
            "date".into(),
            PropertyValue::Date {
                start: "2026-10-07T10:00:00Z".into(),
                end: None,
            },
        )]);
        let mut filters = MetadataFilters {
            properties: Some(vec![PropertyFilter::Equals {
                property_id: "date".into(),
                value: PropertyValue::Date {
                    start: "2026-10-07T12:00:00+02:00".into(),
                    end: None,
                },
            }]),
            ..Default::default()
        };
        filters.validate().unwrap();
        assert!(!filters.matches_values("", &properties).unwrap());
        assert_eq!(
            serde_json::from_value::<MetadataFilters>(serde_json::to_value(&filters).unwrap())
                .unwrap(),
            filters
        );
        filters.properties = Some(vec![PropertyFilter::Equals {
            property_id: "date".into(),
            value: PropertyValue::Date {
                start: "not-a-date".into(),
                end: None,
            },
        }]);
        assert!(filters.validate().is_err());
    }
    #[test]
    fn unsupported_and_malformed_filter_shapes_fail_deserialization() {
        for input in [
            serde_json::json!({"created_time":{"from":"2026-10-07T00:00:00Z"}}),
            serde_json::json!({"edited":null}),
            serde_json::json!({"workspace_ids":null}),
            serde_json::json!({"properties":[{"operator":"regex","property_id":"tags","value":".*"}]}),
            serde_json::json!({"properties":[{"operator":"equals","property_id":"x","value":{"type":"number","value":1,"unknown":true}}]}),
            serde_json::json!({"properties":[{"operator":"equals","property_id":"x","value":{"type":"date","value":{"start":"2026-01-01","end":null,"unknown":true}}}]}),
        ] {
            assert!(serde_json::from_value::<MetadataFilters>(input).is_err());
        }
    }
}
