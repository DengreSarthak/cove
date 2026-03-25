// returns the dollars and the cents (with the decimal point) as a string
pub fn seperate_and_limit_dollars_and_cents(
    amount: &str,
    max_decimal_places: usize,
) -> (&str, &str) {
    // get how many decimals there are after the decimal point
    let last_index = amount.len().saturating_sub(1);

    let decimal_index = match memchr::memchr(b'.', amount.as_bytes()) {
        Some(decimal_index) => decimal_index,
        None => return (amount, ""),
    };

    let current_decimal_places = last_index - decimal_index;

    // get the number of decimals after the decimal point
    let decimal_places = current_decimal_places.min(max_decimal_places);

    let dollars = &amount[..decimal_index];
    let cents_with_decimal_point = &amount[decimal_index..=decimal_index + decimal_places];
    (dollars, cents_with_decimal_point)
}
