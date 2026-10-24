//! What the engine reports: assets, how much of each a wallet holds, and how
//! far it trusts each one.

use registry::model::{ChainInfo, TokenInfo};
use serde::{Deserialize, Serialize};

/// The standard an asset follows. Fungible ones are counted in base units,
/// NFTs one by one (ERC-1155 and ERC-404 by count).
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Standard {
    /// A chain's own coin: ETH, SOL, BTC.
    Native,
    Erc20,
    Erc721,
    Erc1155,
    /// Fungible and NFT at once. A held count without an id is fungible.
    Erc404,
    /// SPL Token or Token-2022.
    Spl,
    /// Metaplex Token Metadata NFT.
    Metaplex,
    /// A Metaplex print edition.
    Edition,
    /// Metaplex programmable NFT (pNFT).
    Programmable,
    /// Metaplex Core asset.
    Core,
    /// Compressed (Bubblegum) NFT.
    Compressed,
}

/// One asset on one chain.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Asset {
    /// Fungible: `evm:{chainId}:native|{contract lower-case}`, `bitcoin:native`,
    /// `solana:{registry id}:native|{mint}`. NFT: `{registry id}:{contract
    /// lower-case}:{token id}` on EVM, `{registry id}:{mint}` on Solana.
    pub id: String,
    /// The registry id of the chain.
    pub chain_id: String,
    pub standard: Standard,
    /// Contract or mint. `None` for the native coin.
    #[serde(default)]
    pub contract: Option<String>,
    /// EVM NFT id, as a decimal string.
    #[serde(default)]
    pub token_id: Option<String>,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
    /// Logo, only ever from the registry's own lists: a link a contract or
    /// an index gives is never trusted with a page load.
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub coingecko_id: Option<String>,
    /// An index (Blockscout, DAS) called it spam.
    #[serde(default)]
    pub flagged: bool,
}

impl Asset {
    /// One NFT (or a count of one ERC-1155 id) rather than a fungible balance.
    pub fn is_nft(&self) -> bool {
        self.token_id.is_some()
            || matches!(
                self.standard,
                Standard::Metaplex | Standard::Edition | Standard::Programmable | Standard::Core | Standard::Compressed
            )
    }
}

/// How far an asset is trusted. The name and symbol of a token prove
/// nothing; only being on a list does.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "level", content = "reason", rename_all = "lowercase")]
pub enum Trust {
    /// The chain's coin, on a shipped list, or added by the user.
    Verified,
    /// Held, but no list names it.
    Unverified,
    /// Shown, with a warning: it takes the symbol of a listed token or of the
    /// chain's coin, on another contract. Often a fake, sometimes a real
    /// bridged or older copy, so it is never hidden for this alone.
    Suspicious(Reason),
    /// Looks like a scam: hidden until the user shows it.
    Spam(Reason),
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Reason {
    /// "Claim your reward", "airdrop", …
    Lure,
    /// A web address in the name or symbol.
    Link,
    /// Invisible or direction-changing characters.
    Invisible,
    /// Takes the symbol of a listed token or the chain's coin.
    Impersonates,
    /// An index flagged it.
    Indexer,
}

/// An asset and how much of it the address holds.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Holding {
    #[serde(flatten)]
    pub asset: Asset,
    /// Base units, as a decimal string.
    pub amount: String,
    pub trust: Trust,
}

impl Holding {
    pub fn held(&self) -> bool {
        self.amount != "0" && !self.amount.is_empty()
    }
}

/// One address on one chain to read.
#[derive(Clone, Debug)]
pub struct Target {
    pub chain: ChainInfo,
    /// Public address. The engine never sees a key.
    pub address: String,
    /// Tokens always reported, even at zero: the wallet's own list for the
    /// chain (defaults, switched-on popular tokens, tokens the user added).
    pub tokens: Vec<TokenInfo>,
    /// Where the last run left off, as returned in an `Update`.
    pub checkpoint: Checkpoint,
}

/// What the engine needs to carry on where it stopped. Holds no amounts:
/// a scan cursor and public asset ids only. The caller stores it.
#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq)]
pub struct Checkpoint {
    /// EVM: the last block scanned for incoming transfers. 0 = never.
    #[serde(default)]
    pub block: u64,
    /// Assets found beyond the target's own list (fungible and NFT).
    #[serde(default)]
    pub found: Vec<Asset>,
    /// Unix seconds of the last pass over an index (Blockscout, DAS).
    #[serde(default)]
    pub indexed_at: u64,
}

/// The holdings of one address on one chain after a round.
#[derive(Serialize, Clone, Debug)]
pub struct Update {
    pub chain_id: String,
    pub address: String,
    /// The full set for this chain: replace, do not merge.
    pub holdings: Vec<Holding>,
    /// Ids whose amount or presence changed since the last update.
    pub changed: Vec<String>,
    pub checkpoint: Checkpoint,
    /// Set when the round failed. `holdings` are then the last good ones
    /// (empty if there were none), never zeros standing in for an answer.
    pub error: Option<String>,
}
