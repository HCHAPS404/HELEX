use std::fs;
use std::io;
use std::path::Path;

use rustc_hash::FxHashMap;

use crate::csv_dialect::{iter_data_lines, parse_fixed_fields};
use crate::timestamp::parse_timestamp;

pub struct Catalog {
    pub window_start: i64,
    pub window_end: i64,
    pub categories: Vec<Vec<u8>>,
    pub unknown_cat_id: u16,
    pub product_to_cat: FxHashMap<Vec<u8>, u16>,
    pub campaign_ids: Vec<Vec<u8>>,
    pub campaign_channels: Vec<Vec<u8>>,
    pub organic_camp_id: u16,
    pub campaign_to_id: FxHashMap<Vec<u8>, u16>,
}

fn extract_json_string<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\"");
    let start = json.find(&needle)? + needle.len();
    let rest = json[start..].trim_start();
    let rest = rest.strip_prefix(':')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

impl Catalog {
    pub fn load(data_dir: &Path) -> io::Result<Self> {
        let manifest_str = fs::read_to_string(data_dir.join("manifest.json"))?;
        let ws_str = extract_json_string(&manifest_str, "window_start")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing window_start"))?;
        let we_str = extract_json_string(&manifest_str, "window_end")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing window_end"))?;
        let window_start = parse_timestamp(ws_str.as_bytes())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_start"))?;
        let window_end = parse_timestamp(we_str.as_bytes())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_end"))?;

        let products_bytes = fs::read(data_dir.join("products.csv"))?;
        let mut raw_products: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(50_000);
        let mut cat_set: Vec<Vec<u8>> = Vec::with_capacity(32);
        cat_set.push(b"unknown".to_vec());

        iter_data_lines(&products_bytes, |line| {
            if let Some([prod_id, cat]) = parse_fixed_fields::<2>(line) {
                if !prod_id.is_empty() && !cat.is_empty() {
                    if !cat_set.iter().any(|c| c.as_slice() == cat) {
                        cat_set.push(cat.to_vec());
                    }
                    raw_products.push((prod_id.to_vec(), cat.to_vec()));
                }
            }
        });

        cat_set.sort_unstable();
        cat_set.dedup();
        let cat_lookup: FxHashMap<Vec<u8>, u16> = cat_set
            .iter()
            .enumerate()
            .map(|(idx, c)| (c.clone(), idx as u16))
            .collect();
        let unknown_cat_id = cat_lookup[b"unknown".as_slice()];

        let mut product_to_cat =
            FxHashMap::with_capacity_and_hasher(raw_products.len(), Default::default());
        for (prod, cat) in raw_products {
            product_to_cat.insert(prod, cat_lookup[&cat]);
        }

        let campaigns_bytes = fs::read(data_dir.join("campaigns.csv"))?;
        let mut campaign_ids: Vec<Vec<u8>> = Vec::with_capacity(2_048);
        let mut campaign_channels: Vec<Vec<u8>> = Vec::with_capacity(2_048);
        let mut campaign_to_id: FxHashMap<Vec<u8>, u16> =
            FxHashMap::with_capacity_and_hasher(2_048, Default::default());

        iter_data_lines(&campaigns_bytes, |line| {
            if let Some([cid, chan]) = parse_fixed_fields::<2>(line) {
                if !cid.is_empty() && !campaign_to_id.contains_key(cid) {
                    let id = campaign_ids.len() as u16;
                    campaign_ids.push(cid.to_vec());
                    campaign_channels.push(chan.to_vec());
                    campaign_to_id.insert(cid.to_vec(), id);
                }
            }
        });

        let organic_camp_id = campaign_ids.len() as u16;
        campaign_ids.push(b"organic".to_vec());
        campaign_channels.push(b"organic".to_vec());

        Ok(Self {
            window_start,
            window_end,
            categories: cat_set,
            unknown_cat_id,
            product_to_cat,
            campaign_ids,
            campaign_channels,
            organic_camp_id,
            campaign_to_id,
        })
    }

    #[inline]
    pub fn category_of(&self, product_id: &[u8]) -> u16 {
        self.product_to_cat
            .get(product_id)
            .copied()
            .unwrap_or(self.unknown_cat_id)
    }

    #[inline]
    pub fn campaign_of(&self, campaign_id: &[u8]) -> Option<u16> {
        self.campaign_to_id.get(campaign_id).copied()
    }
}
