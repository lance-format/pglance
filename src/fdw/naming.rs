pub fn sanitize_ident(input: &str) -> String {
    let mut out = String::with_capacity(input.len());

    let mut last_was_underscore = false;
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        let ok = matches!(lower, 'a'..='z' | '0'..='9' | '_');
        let mapped = if ok { lower } else { '_' };

        if mapped == '_' {
            if last_was_underscore {
                continue;
            }
            last_was_underscore = true;
        } else {
            last_was_underscore = false;
        }

        out.push(mapped);
    }

    while out.starts_with('_') {
        out.remove(0);
    }
    while out.ends_with('_') {
        out.pop();
    }

    if out.is_empty() {
        out.push('_');
    }

    if out.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
        out.insert(0, '_');
    }

    if out.len() > 63 {
        out.truncate(63);
    }

    out
}

pub fn schema_and_table_for_table_id(
    schema_prefix: &str,
    table_id: &[String],
) -> Result<(String, String), String> {
    if table_id.is_empty() {
        return Err("table_id must not be empty".to_string());
    }

    let prefix = sanitize_ident(schema_prefix);
    let table = sanitize_ident(table_id.last().expect("non-empty"));

    let namespace = &table_id[..table_id.len() - 1];
    if namespace.is_empty() {
        return Ok((prefix, table));
    }

    let mut schema = String::with_capacity(prefix.len() + 2 + table_id.len() * 8);
    schema.push_str(&prefix);
    for seg in namespace {
        schema.push_str("__");
        schema.push_str(&sanitize_ident(seg));
    }

    if schema.len() > 63 {
        schema.truncate(63);
    }

    Ok((schema, table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_ident_basic() {
        assert_eq!(sanitize_ident("HelloWorld"), "helloworld");
        assert_eq!(sanitize_ident("a-b-c"), "a_b_c");
        assert_eq!(sanitize_ident("a---b"), "a_b");
        assert_eq!(sanitize_ident("__a__"), "a");
        assert_eq!(sanitize_ident(""), "_");
        assert_eq!(sanitize_ident("123"), "_123");
        assert_eq!(sanitize_ident("a.b$c"), "a_b_c");
    }

    #[test]
    fn test_schema_and_table_for_table_id_root() {
        let (schema, table) =
            schema_and_table_for_table_id("lance", &[String::from("t")]).expect("mapping");
        assert_eq!(schema, "lance");
        assert_eq!(table, "t");
    }

    #[test]
    fn test_schema_and_table_for_table_id_nested() {
        let id = vec![
            "TeamA".to_string(),
            "images".to_string(),
            "train".to_string(),
        ];
        let (schema, table) = schema_and_table_for_table_id("lance", &id).expect("mapping");
        assert_eq!(schema, "lance__teama__images");
        assert_eq!(table, "train");
    }
}
