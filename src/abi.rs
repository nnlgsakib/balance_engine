//! The few Solidity calls the engine makes, encoded and decoded by hand.
//!
//! Every token answers the same fixed selectors, so a full ABI library is not
//! needed, and nothing here depends on one. What a contract returns is
//! untrusted: strings are length-capped and cleaned, numbers bounded.

/// Multicall3, at the same address on nearly every EVM chain.
pub const MULTICALL3: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

pub const BALANCE_OF: [u8; 4] = [0x70, 0xa0, 0x82, 0x31]; // balanceOf(address)
pub const BALANCE_OF_ID: [u8; 4] = [0x00, 0xfd, 0xd5, 0x8e]; // balanceOf(address,uint256)
pub const OWNER_OF: [u8; 4] = [0x63, 0x52, 0x21, 0x1e]; // ownerOf(uint256)
pub const TOKEN_OF_OWNER_BY_INDEX: [u8; 4] = [0x2f, 0x74, 0x5c, 0x59]; // tokenOfOwnerByIndex(address,uint256)
pub const SUPPORTS_INTERFACE: [u8; 4] = [0x01, 0xff, 0xc9, 0xa7]; // supportsInterface(bytes4)
pub const NAME: [u8; 4] = [0x06, 0xfd, 0xde, 0x03];
pub const SYMBOL: [u8; 4] = [0x95, 0xd8, 0x9b, 0x41];
pub const DECIMALS: [u8; 4] = [0x31, 0x3c, 0xe5, 0x67];
pub const TOTAL_SUPPLY: [u8; 4] = [0x18, 0x16, 0x0d, 0xdd];
pub const GET_ETH_BALANCE: [u8; 4] = [0x4d, 0x23, 0x01, 0xcc]; // Multicall3.getEthBalance(address)
const AGGREGATE3: [u8; 4] = [0x82, 0xad, 0x56, 0xcb]; // aggregate3((address,bool,bytes)[])

/// ERC-165 interface ids.
pub const ERC721: [u8; 4] = [0x80, 0xac, 0x58, 0xcd];
pub const ERC1155: [u8; 4] = [0xd9, 0xb6, 0x7a, 0x26];

/// Event topics.
pub const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const TRANSFER_SINGLE: &str = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
pub const TRANSFER_BATCH: &str = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

/// Longest contract string kept. Real names and symbols are far shorter.
const MAX_STRING_LEN: usize = 64;

pub type Word = [u8; 32];

pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

pub fn unhex(text: &str) -> Option<Vec<u8>> {
    let digits = text.strip_prefix("0x").unwrap_or(text);
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    (0..digits.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(digits.get(i..i + 2)?, 16).ok())
        .collect()
}

/// `0x` + 40 hex digits, as a left-padded word.
pub fn address_word(address: &str) -> Option<Word> {
    let bytes = unhex(address.trim())?;
    if bytes.len() != 20 {
        return None;
    }
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(&bytes);
    Some(word)
}

/// An address as a log topic.
pub fn address_topic(address: &str) -> Option<String> {
    address_word(address).map(|w| hex(&w))
}

/// The address in the low 20 bytes of a word, lower-case.
pub fn word_address(word: &[u8]) -> Option<String> {
    (word.len() == 32 && word[..12].iter().all(|b| *b == 0)).then(|| hex(&word[12..]))
}

pub fn usize_word(value: usize) -> Word {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&(value as u64).to_be_bytes());
    word
}

/// A decimal string (a token id, an amount) as a word. `None` past u256.
pub fn dec_word(decimal: &str) -> Option<Word> {
    let digits = decimal.trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut word = [0u8; 32];
    for d in digits.bytes() {
        let mut carry = u32::from(d - b'0');
        for byte in word.iter_mut().rev() {
            let v = u32::from(*byte) * 10 + carry;
            *byte = (v & 0xff) as u8;
            carry = v >> 8;
        }
        if carry != 0 {
            return None;
        }
    }
    Some(word)
}

/// A big-endian number of any length (up to u256) as a decimal string.
pub fn word_dec(bytes: &[u8]) -> String {
    let mut n: Vec<u8> = bytes.iter().copied().skip_while(|b| *b == 0).collect();
    if n.is_empty() {
        return "0".into();
    }
    let mut digits = Vec::new();
    while !n.is_empty() {
        let mut rem = 0u32;
        let mut quotient = Vec::with_capacity(n.len());
        for &b in &n {
            let cur = rem * 256 + u32::from(b);
            let d = cur / 10;
            rem = cur % 10;
            if !(quotient.is_empty() && d == 0) {
                quotient.push(d as u8);
            }
        }
        digits.push(b'0' + rem as u8);
        n = quotient;
    }
    digits.reverse();
    String::from_utf8(digits).unwrap_or_else(|_| "0".into())
}

/// The first word of a return value as a decimal amount. An empty answer
/// (a call to an address with no code) is 0.
pub fn amount(returndata: &[u8]) -> String {
    returndata.get(..32).map_or_else(|| "0".into(), word_dec)
}

/// Calldata: selector plus word arguments.
pub fn call(selector: [u8; 4], args: &[Word]) -> Vec<u8> {
    let mut out = selector.to_vec();
    for arg in args {
        out.extend_from_slice(arg);
    }
    out
}

/// A word read as an offset or length; `None` when it cannot be one.
fn read_usize(data: &[u8], offset: usize) -> Option<usize> {
    let word = data.get(offset..offset.checked_add(32)?)?;
    if word[..24].iter().any(|b| *b != 0) {
        return None;
    }
    let value = u64::from_be_bytes(word[24..].try_into().ok()?);
    usize::try_from(value).ok().filter(|v| *v <= u32::MAX as usize)
}

/// One call inside an `aggregate3`.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    pub target: String,
    pub data: Vec<u8>,
}

/// `aggregate3` calldata, every call allowed to fail on its own.
pub fn aggregate3(calls: &[Call]) -> Option<Vec<u8>> {
    let mut items = Vec::with_capacity(calls.len());
    for c in calls {
        let mut item = address_word(&c.target)?.to_vec();
        item.extend_from_slice(&usize_word(1)); // allowFailure
        item.extend_from_slice(&usize_word(96)); // where the bytes start
        item.extend_from_slice(&usize_word(c.data.len()));
        item.extend_from_slice(&c.data);
        item.resize(item.len() + (32 - c.data.len() % 32) % 32, 0);
        items.push(item);
    }
    let mut out = AGGREGATE3.to_vec();
    out.extend_from_slice(&usize_word(32));
    out.extend_from_slice(&usize_word(calls.len()));
    let mut offset = 32 * calls.len();
    for item in &items {
        out.extend_from_slice(&usize_word(offset));
        offset += item.len();
    }
    for item in items {
        out.extend(item);
    }
    Some(out)
}

/// How many calls an `aggregate3` calldata carries. For tests.
#[cfg(test)]
pub fn decode_aggregate3_count(calldata: &[u8]) -> usize {
    read_usize(&calldata[4..], 32).unwrap_or(0)
}

/// `(bool success, bytes data)[]`, as `aggregate3` returns it.
pub fn decode_aggregate3(ret: &[u8]) -> Option<Vec<(bool, Vec<u8>)>> {
    let start = read_usize(ret, 0)?;
    let count = read_usize(ret, start)?;
    let base = start.checked_add(32)?;
    let mut out = Vec::with_capacity(count.min(4096));
    for i in 0..count {
        let item = base.checked_add(read_usize(ret, base.checked_add(i.checked_mul(32)?)?)?)?;
        let success = ret.get(item..item.checked_add(32)?)?.iter().any(|b| *b != 0);
        let data_at = item.checked_add(read_usize(ret, item.checked_add(32)?)?)?;
        let len = read_usize(ret, data_at)?;
        let from = data_at.checked_add(32)?;
        out.push((success, ret.get(from..from.checked_add(len)?)?.to_vec()));
    }
    Some(out)
}

/// `uint256[]` whose offset is in the word at `head_word` of `data` (event
/// data, as in `TransferBatch`). At most `limit` items.
pub fn uint_array(data: &[u8], head_word: usize, limit: usize) -> Option<Vec<String>> {
    let at = read_usize(data, head_word.checked_mul(32)?)?;
    let count = read_usize(data, at)?;
    (0..count.min(limit))
        .map(|i| {
            let from = at.checked_add(32)?.checked_add(i.checked_mul(32)?)?;
            Some(word_dec(data.get(from..from.checked_add(32)?)?))
        })
        .collect()
}

/// A Solidity `string`: the ABI dynamic form, or the old `bytes32` form some
/// tokens (MKR) still return.
pub fn decode_string(returndata: &[u8]) -> Option<String> {
    let bytes = if returndata.len() >= 64 {
        let offset = read_usize(returndata, 0)?;
        let length = read_usize(returndata, offset)?;
        if length > 4 * MAX_STRING_LEN {
            return None;
        }
        let start = offset.checked_add(32)?;
        returndata.get(start..start.checked_add(length)?)?
    } else {
        returndata
    };
    sanitize(&String::from_utf8_lossy(bytes))
}

/// Trims padding, drops control characters, caps the length. `None` if
/// nothing is left.
pub fn sanitize(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .trim_matches(|c: char| c.is_whitespace() || c == '\u{0}')
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_STRING_LEN)
        .collect();
    let trimmed = cleaned.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// `decimals()`, rejecting what no wallet can show (more than 36).
pub fn decode_decimals(returndata: &[u8]) -> Option<u8> {
    let value = read_usize(returndata, 0)?;
    (value <= 36).then_some(value as u8)
}

pub fn decode_bool(returndata: &[u8]) -> Option<bool> {
    let word = returndata.get(..32)?;
    Some(word.iter().any(|b| *b != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_round_trip_up_to_u256() {
        let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        assert_eq!(word_dec(&dec_word(max).unwrap()), max);
        assert!(dec_word("115792089237316195423570985008687907853269984665640564039457584007913129639936").is_none());
        assert_eq!(word_dec(&dec_word("0").unwrap()), "0");
        assert_eq!(word_dec(&dec_word("1000000").unwrap()), "1000000");
        assert_eq!(dec_word("12a"), None);
        assert_eq!(amount(&[]), "0", "an address with no code answers nothing: 0");
    }

    /// The layout of `aggregate3([(0x11…, true, 0xaabb), (0x22…, true, 0x)])`,
    /// word by word as the ABI defines it.
    #[test]
    fn aggregate3_is_encoded_as_the_abi_says() {
        let a = "0x1111111111111111111111111111111111111111";
        let b = "0x2222222222222222222222222222222222222222";
        let data = aggregate3(&[
            Call { target: a.into(), data: vec![0xaa, 0xbb] },
            Call { target: b.into(), data: vec![] },
        ])
        .unwrap();
        assert_eq!(&data[..4], &[0x82, 0xad, 0x56, 0xcb]);
        let words: Vec<String> = data[4..].chunks(32).map(|w| hex(w)[2..].trim_start_matches('0').to_string()).collect();
        let want = [
            "20",                                       // array at 0x20
            "2",                                        // two calls
            "40",                                       // first tuple after the two offsets
            "e0",                                       // second: 0x40 + the first's 5 words
            "1111111111111111111111111111111111111111", // target
            "1",                                        // allowFailure
            "60",                                       // bytes after 3 head words
            "2",                                        // two bytes
            "aabb000000000000000000000000000000000000000000000000000000000000",
            "2222222222222222222222222222222222222222",
            "1",
            "60",
            "", // zero-length bytes
        ];
        assert_eq!(words, want);
    }

    #[test]
    fn aggregate3_results_decode_including_failures() {
        // [(true, word 5), (false, 0x)]
        let mut ret = Vec::new();
        for w in [32usize, 2, 64, 192, 1, 64, 32, 5, 0, 64, 0] {
            ret.extend_from_slice(&usize_word(w));
        }
        let out = decode_aggregate3(&ret).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].0);
        assert_eq!(amount(&out[0].1), "5");
        assert_eq!(out[1], (false, vec![]));
        assert_eq!(decode_aggregate3(&ret[..100]), None, "a cut answer is refused, not misread");
    }

    #[test]
    fn strings_decode_in_both_forms_and_are_cleaned() {
        let mut abi = usize_word(32).to_vec();
        abi.extend_from_slice(&usize_word(4));
        let mut text = b"USDC".to_vec();
        text.resize(32, 0);
        abi.extend(text);
        assert_eq!(decode_string(&abi).as_deref(), Some("USDC"));
        // MKR: bytes32, zero-padded.
        let mut mkr = b"MKR".to_vec();
        mkr.resize(32, 0);
        assert_eq!(decode_string(&mkr).as_deref(), Some("MKR"));
        assert_eq!(sanitize("  \u{0}\u{7}  "), None);
        assert_eq!(sanitize(&"x".repeat(500)).map(|s| s.len()), Some(64));
    }

    #[test]
    fn decimals_past_36_are_refused() {
        assert_eq!(decode_decimals(&usize_word(18)), Some(18));
        assert_eq!(decode_decimals(&usize_word(37)), None);
        assert_eq!(decode_decimals(&[0xff; 32]), None);
    }

    #[test]
    fn addresses_and_batch_ids_decode() {
        let topic = address_topic("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045").unwrap();
        assert_eq!(topic, "0x000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045");
        assert_eq!(word_address(&unhex(&topic).unwrap()).as_deref(), Some("0xd8da6bf26964af9d7eed9e03e53415d37aa96045"));
        assert!(address_word("0x1234").is_none());
        // TransferBatch data: ids [7, 9], values [1, 2]
        let mut data = Vec::new();
        for w in [64usize, 160, 2, 7, 9, 2, 1, 2] {
            data.extend_from_slice(&usize_word(w));
        }
        assert_eq!(uint_array(&data, 0, 50).unwrap(), ["7", "9"]);
        assert_eq!(uint_array(&data, 0, 1).unwrap(), ["7"]);
    }
}
