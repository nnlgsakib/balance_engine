//! How far a found asset is trusted. Anyone can deploy a token with any name
//! and send it to any wallet, so only a list makes a token verified.
//!
//! As in Phantom and Rabby, a copied symbol is a warning, not a verdict: a
//! token named "USDT" on another contract is often a fake, and sometimes a
//! real bridged or older copy. It is shown as suspicious, unpriced and out of
//! the total. Only lures are hidden as spam: an index's flag, a link, bait
//! words, or invisible characters.

use crate::model::{Asset, Reason, Standard, Trust};
use registry::model::{ChainInfo, TokenInfo};

/// Words that mark "claim your reward" airdrops used for phishing.
const LURES: &[&str] = &["claim", "reward", "airdrop", "voucher", "visit ", "redeem", "giveaway", "bonus", "free mint", "$"];

const TLDS: &[&str] = &[".com", ".io", ".xyz", ".net", ".org", ".app", ".gift", ".site", ".fun", ".finance", ".cc", ".co", ".me", ".pro", ".top", ".vip", ".online", ".live"];

fn has_link(text: &str) -> bool {
    let text = text.to_lowercase();
    text.contains("http")
        || text.contains("www.")
        || text.contains("t.me/")
        || text.split_whitespace().any(|w| {
            let w = w.trim_matches(|c: char| !c.is_alphanumeric() && c != '.');
            TLDS.iter().any(|tld| w.len() > tld.len() && w.ends_with(tld))
        })
}

fn has_lure(text: &str) -> bool {
    let text = text.to_lowercase();
    LURES.iter().any(|w| text.contains(w))
}

/// Zero-width, bidi and other invisible characters: used to make one name
/// look like another.
fn has_invisible(text: &str) -> bool {
    text.chars().any(|c| {
        matches!(c, '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}' | '\u{00AD}' | '\u{2060}')
    })
}

/// A guess at whether an NFT is a scam airdrop: its name or description
/// pushes a link and a reward, or its name is itself a web address. Wrong
/// now and then; the user can unhide.
pub fn looks_like_spam(name: &str, description: Option<&str>) -> bool {
    let text = format!("{} {}", name, description.unwrap_or(""));
    let link = has_link(&text);
    link && has_lure(&text) || (link && name.contains('.'))
}

/// A symbol reduced to what a reader compares: ASCII letters and digits,
/// upper-case. `USDC`, `usdc`, `U.S.D.C` are one symbol.
// ponytail: ASCII only; look-alike letters from other scripts (Cyrillic "С")
// pass as different. Add a confusables table if such spam gets through.
fn folded(symbol: &str) -> String {
    symbol.chars().filter(char::is_ascii_alphanumeric).map(|c| c.to_ascii_uppercase()).collect()
}

fn same_address(chain: &ChainInfo, a: &str, b: &str) -> bool {
    match chain {
        // A mint is case-sensitive.
        ChainInfo::Solana(_) => a == b,
        _ => a.eq_ignore_ascii_case(b),
    }
}

/// The tokens a chain trusts by list: its defaults and its popular list.
fn listed(chain: &ChainInfo) -> Vec<&TokenInfo> {
    let defaults: &[TokenInfo] = match chain {
        ChainInfo::Evm(c) => &c.default_tokens,
        ChainInfo::Solana(c) => &c.default_tokens,
        ChainInfo::Bitcoin(_) => &[],
    };
    let popular = registry::popular::chain_key(chain).map_or(&[][..], |k| registry::popular::popular_tokens(&k));
    defaults.iter().chain(popular).collect()
}

/// Whether a list (or the user) vouches for this contract.
pub(crate) fn is_listed(asset: &Asset, chain: &ChainInfo, tokens: &[TokenInfo]) -> bool {
    let Some(contract) = asset.contract.as_deref() else { return false };
    listed(chain).iter().any(|t| same_address(chain, &t.address, contract))
        || tokens.iter().any(|t| (t.custom || !t.discovered) && same_address(chain, &t.address, contract))
}

/// Trust for one asset on `chain`. `tokens` is the wallet's own list for the
/// chain (custom tokens count as vouched for by the user).
pub fn classify(asset: &Asset, chain: &ChainInfo, tokens: &[TokenInfo]) -> Trust {
    if asset.standard == Standard::Native {
        return Trust::Verified;
    }
    if !asset.is_nft() && is_listed(asset, chain, tokens) {
        return Trust::Verified;
    }
    let text = format!("{} {}", asset.symbol, asset.name);
    if asset.flagged {
        return Trust::Spam(Reason::Indexer);
    }
    if has_invisible(&text) {
        return Trust::Spam(Reason::Invisible);
    }
    if has_link(&text) {
        return Trust::Spam(Reason::Link);
    }
    if has_lure(&text) {
        return Trust::Spam(Reason::Lure);
    }
    if !asset.is_nft() {
        let symbol = folded(&asset.symbol);
        let native = folded(&chain.native_currency().symbol);
        let copies = !symbol.is_empty()
            && (symbol == native || listed(chain).iter().any(|t| folded(&t.symbol) == symbol));
        if copies {
            return Trust::Suspicious(Reason::Impersonates);
        }
    }
    Trust::Unverified
}

#[cfg(test)]
mod tests {
    use super::*;
    use registry::model::TokenType;

    fn chain(id: &str) -> ChainInfo {
        registry::chains::builtin_only().into_iter().find(|c| c.id() == id).expect("built-in chain")
    }

    fn asset(contract: &str, symbol: &str, name: &str) -> Asset {
        Asset {
            id: format!("evm:1:{}", contract.to_lowercase()),
            chain_id: "ethereum".into(),
            standard: Standard::Erc20,
            contract: Some(contract.into()),
            token_id: None,
            symbol: symbol.into(),
            name: name.into(),
            decimals: 18,
            icon: None,
            coingecko_id: None,
            flagged: false,
        }
    }

    #[test]
    fn trust_follows_the_lists_not_the_name() {
        let eth = chain("ethereum");
        let usdt = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
        let other = "0x1111111111111111111111111111111111111111";
        let cases = [
            (asset(usdt, "USDT", "Tether USD"), Trust::Verified),
            (asset(&usdt.to_lowercase(), "USDT", "Tether USD"), Trust::Verified),
            (asset(other, "USDT", "Tether USD"), Trust::Suspicious(Reason::Impersonates)),
            (asset(other, "U.S.D.T", "Tether"), Trust::Suspicious(Reason::Impersonates)),
            (asset(other, "ETH", "Ether"), Trust::Suspicious(Reason::Impersonates)),
            // A lookalike that also baits is spam.
            (asset(other, "USDT", "Claim your USDT at usdt-gift.com"), Trust::Spam(Reason::Link)),
            (asset(other, "Visit eth-gift.com", "x"), Trust::Spam(Reason::Link)),
            (asset(other, "CLAIM", "Claim your reward now"), Trust::Spam(Reason::Lure)),
            (asset(other, "US\u{200B}DT", "Tether"), Trust::Spam(Reason::Invisible)),
            (asset(other, "FROG", "Frog Coin"), Trust::Unverified),
            (Asset { flagged: true, ..asset(other, "FROG", "Frog Coin") }, Trust::Spam(Reason::Indexer)),
        ];
        for (a, want) in cases {
            assert_eq!(classify(&a, &eth, &[]), want, "{} / {}", a.symbol, a.name);
        }
    }

    #[test]
    fn a_token_the_user_added_is_verified_and_a_found_one_is_not() {
        let eth = chain("ethereum");
        let contract = "0x2222222222222222222222222222222222222222";
        let mut token = TokenInfo {
            address: contract.into(),
            symbol: "FROG".into(),
            name: "Frog".into(),
            decimals: 18,
            token_type: TokenType::Erc20,
            coingecko_id: None,
            icon: None,
            custom: true,
            discovered: false,
        };
        assert_eq!(classify(&asset(contract, "FROG", "Frog"), &eth, std::slice::from_ref(&token)), Trust::Verified);
        token.custom = false;
        token.discovered = true;
        assert_eq!(classify(&asset(contract, "FROG", "Frog"), &eth, &[token]), Trust::Unverified);
    }

    #[test]
    fn a_solana_mint_is_compared_exactly() {
        let sol = chain("solana");
        let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
        let mut a = asset(usdc, "USDC", "USD Coin");
        a.standard = Standard::Spl;
        assert_eq!(classify(&a, &sol, &[]), Trust::Verified);
        a.contract = Some(usdc.to_lowercase());
        assert_eq!(classify(&a, &sol, &[]), Trust::Suspicious(Reason::Impersonates));
    }

    #[test]
    fn nft_spam_needs_a_link() {
        assert!(looks_like_spam("Claim at nft-drop.xyz", None));
        assert!(looks_like_spam("Bored Ape", Some("Visit https://x.example to claim your reward")));
        assert!(!looks_like_spam("Bored Ape #1", Some("A reward for holders")));
    }
}
