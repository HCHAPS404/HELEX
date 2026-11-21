use std::fs;
use std::io;
use std::path::Path;

use hashbrown::HashTable;

use crate::csv_dialect::{iter_data_lines, parse_fixed_fields};
use crate::sso::{hash64_bytes, SmallBytes};
use crate::timestamp::parse_timestamp;

pub struct Catalog {
    pub window_start: i64,
    pub window_end: i64,
    pub categories: Vec<Vec<u8>>,
    pub unknown_cat_id: u16,
    product_keys: Vec<SmallBytes>,
    product_cats: Vec<u16>,
    product_table: HashTable<u32>,
    pub campaign_ids: Vec<Vec<u8>>,
    pub campaign_channels: Vec<Vec<u8>>,
    pub organic_camp_id: u16,
    campaign_keys: Vec<SmallBytes>,
    campaign_table: HashTable<u16>,
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
        let mut raw_products: Vec<(SmallBytes, Vec<u8>)> = Vec::with_capacity(50_000);
        let mut cat_set: Vec<Vec<u8>> = Vec::with_capacity(32);
        cat_set.push(b"unknown".to_vec());

        iter_data_lines(&products_bytes, |line| {
            if let Some([prod_id, cat]) = parse_fixed_fields::<2>(line) {
                if !prod_id.is_empty() && !cat.is_empty() {
                    if !cat_set.iter().any(|c| c.as_slice() == cat) {
                        cat_set.push(cat.to_vec());
                    }
                    raw_products.push((SmallBytes::from_slice(prod_id), cat.to_vec()));
                }
            }
        });

        cat_set.sort_unstable();
        cat_set.dedup();
        let unknown_cat_id = cat_set
            .iter()
            .position(|c| c.as_slice() == b"unknown")
            .unwrap() as u16;

        let mut product_keys = Vec::with_capacity(raw_products.len());
        let mut product_cats = Vec::with_capacity(raw_products.len());
        let mut product_table = HashTable::with_capacity(raw_products.len());

        for (prod, cat) in raw_products {
            let cid = cat_set.binary_search(&cat).unwrap() as u16;
            let h = prod.hash64();
            if product_table
                .find(h, |&idx: &u32| product_keys[idx as usize] == prod)
                .is_none()
            {
                let idx = product_keys.len() as u32;
                product_keys.push(prod);
                product_cats.push(cid);
                product_table.insert_unique(h, idx, |&i| product_keys[i as usize].hash64());
            }
        }

        let campaigns_bytes = fs::read(data_dir.join("campaigns.csv"))?;
        let mut campaign_ids: Vec<Vec<u8>> = Vec::with_capacity(2_048);
        let mut campaign_channels: Vec<Vec<u8>> = Vec::with_capacity(2_048);
        let mut campaign_keys: Vec<SmallBytes> = Vec::with_capacity(2_048);
        let mut campaign_table: HashTable<u16> = HashTable::with_capacity(2_048);

        iter_data_lines(&campaigns_bytes, |line| {
            if let Some([cid, chan]) = parse_fixed_fields::<2>(line) {
                if !cid.is_empty() {
                    let h = hash64_bytes(cid);
                    if campaign_table
                        .find(h, |&id: &u16| campaign_keys[id as usize].eq_slice(cid))
                        .is_none()
                    {
                        let id = campaign_ids.len() as u16;
                        campaign_ids.push(cid.to_vec());
                        campaign_channels.push(chan.to_vec());
                        campaign_keys.push(SmallBytes::from_slice(cid));
                        campaign_table
                            .insert_unique(h, id, |&i| campaign_keys[i as usize].hash64());
                    }
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
            product_keys,
            product_cats,
            product_table,
            campaign_ids,
            campaign_channels,
            organic_camp_id,
            campaign_keys,
            campaign_table,
        })
    }

    #[inline]
    pub fn category_of(&self, product_id: &[u8]) -> u16 {
        let h = hash64_bytes(product_id);
        self.product_table
            .find(h, |&idx| self.product_keys[idx as usize].eq_slice(product_id))
            .map_or(self.unknown_cat_id, |&idx| self.product_cats[idx as usize])
    }

    #[inline]
    pub fn campaign_of(&self, campaign_id: &[u8]) -> Option<u16> {
        let h = hash64_bytes(campaign_id);
        self.campaign_table
            .find(h, |&id| self.campaign_keys[id as usize].eq_slice(campaign_id))
            .copied()
    }
}
