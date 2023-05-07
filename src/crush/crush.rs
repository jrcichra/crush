extern crate alloc;

use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::{
    hash::{Hash, Hasher},
    panic,
};
use std::collections::hash_map::DefaultHasher;

lazy_static::lazy_static! {
    /// The ln table with value ln(x)<<44 for x in [0,65536).
    static ref LN_TABLE: Vec<u64> =
        (0..65536).map(|i| (-((i as f64 / 65536.0).ln() * ((1u64 << 44) as f64)).round()) as u64).collect();
}

/// The CRUSH algorithm.
#[derive(Default, Clone)]
pub struct Crush {
    root: Node,
}

/// A node in cluster map.
///
/// Maybe root / row / rack / host / osd.
#[derive(Default, Clone)]
struct Node {
    weight: u64,
    out: bool,
    _type: String,
    children: BTreeMap<String, Node>,
}

impl Crush {
    /// Add weight to a node.
    pub fn add_weight(&mut self, path: &str, weight: i64) {
        self.root.add_weight(path, weight);
    }

    /// Locate a node by `pgid`.
    pub fn locate(&self, pgid: u32) -> String {
        self.select(pgid, 1, "/").into_iter().next().unwrap()
    }

    /// Return the total weight of the cluster.
    pub fn total_weight(&self) -> u64 {
        self.root.weight
    }

    /// Get the weight of a node.
    pub fn get_weight(&self, path: &str) -> u64 {
        self.root.get(path).weight
    }

    /// Set a node IN/OUT.
    pub fn set_inout(&mut self, path: &str, out: bool) {
        self.root.get_mut(path).out = out;
    }

    /// Get IN/OUT of a node.
    pub fn get_inout(&self, path: &str) -> bool {
        self.root.get(path).out
    }

    fn get_node_by_path(&self, path: &str) -> &Node {
        self.root.get(path)
    }

    /// Select `num` targets accoding to `pgid`.
    pub fn select(&self, pgid: u32, num: u32, start_path: &str) -> Vec<String> {
        let mut targets = Vec::<String>::new();
        let mut failure_count = 0;

        // determine the node we should start with based on the path.
        let start = self.get_node_by_path(start_path);

        for r in 0..num {
            let mut node = start;
            let mut local_failure = 0;
            let mut fullname = String::new();
            loop {
                let name = node.choose(pgid, r + failure_count);
                if !fullname.is_empty() {
                    fullname += "/";
                }
                fullname += name;
                let child = &node.children[name];
                // if !child.children.is_empty() {
                //     node = child;
                //     continue;
                // }
                if !child.out && !targets.contains(&fullname) {
                    // found one
                    break;
                }
                // chop off the failed node from the fullname
                fullname = fullname
                    .strip_suffix(&format!("/{}", name))
                    .unwrap_or_default()
                    .to_string();

                failure_count += 1;
                local_failure += 1;
                if local_failure > 3 {
                    node = &self.root;
                    local_failure = 0;
                    fullname.clear();
                }
            }
            targets.push(fullname);
        }
        targets
    }
}

impl Node {
    /// Add weight to a node.
    fn add_weight(&mut self, path: &str, weight: i64) {
        self.weight = (self.weight as i64 + weight) as u64;
        if path.is_empty() {
            return;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        let child = self.children.entry(name.into()).or_default();
        child.add_weight(suffix, weight);
    }

    /// Get a node by path.
    fn get(&self, path: &str) -> &Self {
        if path.is_empty() {
            return self;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        self.children[name].get(suffix)
    }

    /// Get a mutable node by path.
    fn get_mut(&mut self, path: &str) -> &mut Self {
        if path.is_empty() {
            return self;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        self.children.get_mut(name).unwrap().get_mut(suffix)
    }

    /// Choose a child according to key and index.
    fn choose(&self, key: u32, index: u32) -> &str {
        self.children
            .iter()
            .map(|(name, child)| {
                let mut hasher = DefaultHasher::new();
                name.hash(&mut hasher);
                key.hash(&mut hasher);
                index.hash(&mut hasher);

                let hash = hasher.finish() & 65535;

                let w = LN_TABLE[hash as usize] / child.weight;
                (name, w)
            })
            .min_by_key(|(_, w)| *w)
            .unwrap()
            .0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use alloc::format;
    use rand::Rng;

    // /// Generate a 9*9*9*10 cluster map.
    // fn gen_test_map() -> Crush {
    //     // let mut rng = rand::thread_rng();
    //     let mut crush = Crush::default();
    //     for i in 0..9 {
    //         for j in 0..9 {
    //             for k in 0..9 {
    //                 for l in 0..10 {
    //                     let path = path_from_nums(i, j, k, l);
    //                     // let weight = rng.gen_range(1..5);
    //                     crush.add_weight(&path, 1);
    //                 }
    //             }
    //         }
    //     }
    //     crush
    // }

    // fn path_from_nums(i: usize, j: usize, k: usize, l: usize) -> String {
    //     let row = i;
    //     let rack = row * 9 + j;
    //     let host = rack * 9 + k;
    //     let osd = host * 9 + l;
    //     format!("row.{row}/rack.{rack}/host.{host}/osd.{osd}")
    // }

    // #[test]
    // fn basic_balance() {
    //     let crush = gen_test_map();
    //     let mut count = BTreeMap::<String, u32>::new();
    //     let n = 1000000;
    //     for i in 0..n {
    //         let path = crush.locate(i);
    //         *count.entry(path).or_default() += 1;
    //     }
    //     let avg = n / (9 * 9 * 9 * 10);
    //     for (name, count) in count {
    //         let range = avg / 2..avg * 2;
    //         assert!(
    //             range.contains(&count),
    //             "path {name:?} count {count} out of range {range:?}"
    //         );
    //     }
    // }

    fn build_single_node_cluster(osds: u32) -> Crush {
        let mut c = Crush::default();
        for osd in 1..=osds {
            let path = format!("osd.{}", osd);
            c.add_weight(&path, 1);
        }
        c
    }

    fn build_ha_cluster(hosts: u32, osds: u32) -> Crush {
        let mut c = Crush::default();
        for host in 1..=hosts {
            for osd in 1..=osds {
                let path = format!("host.{}/osd.{}", host, osd);
                c.add_weight(&path, 1);
            }
        }
        c
    }

    fn build_datacenter_cluster(racks: u32, hosts: u32, osds: u32) -> Crush {
        let mut c = Crush::default();
        for rack in 1..=racks {
            for host in 1..=hosts {
                for osd in 1..=osds {
                    let path = format!("rack.{}/host.{}/osd.{}", rack, host, osd);
                    c.add_weight(&path, 1);
                }
            }
        }
        c
    }

    #[test]
    fn ha_diverse_and_spread() {
        // simulate a replicas: 3 cluster with a failure domain of "host" in a homelab environment
        let num_of_pgs = 10000;
        let replicas = 3;

        let hosts = 3;
        let osds = 5;

        let c = build_ha_cluster(hosts, osds);

        let mut count = HashMap::new();

        for pg in 1..=num_of_pgs {
            let hosts = c.select(pg, replicas, "");
            let mut placement = vec![];

            for host in hosts {
                let osds = c.select(pg, 1, &host);
                placement.push(format!("{}/{}", host, osds[0]))
            }

            // put all the pg placements into a hashmap for counting spread
            for p in &placement {
                if let Some(x) = count.get_mut(p) {
                    *x += 1;
                } else {
                    count.insert(p.clone(), 1);
                }
            }

            // validate each pg is in three unique hosts
            let set: HashSet<String> = placement
                .iter()
                .map(|x| x.split_once("/").unwrap().0.to_string())
                .collect();
            assert!(set.len() == 3);
        }

        let exact_percentage = 100.0 / (hosts * osds) as f64;

        // make sure the actual is within a percent of the theoretical
        for (_, c) in count {
            let actual_percentage = c as f64 / (num_of_pgs * replicas) as f64 * 100.0;
            assert!(actual_percentage - exact_percentage < 1.0);
        }
    }

    #[test]
    fn rack_diverse_and_spread() {
        // simulate a replicas: 3 cluster with a failure domain of "rack" in a small datacenter
        let num_of_pgs = 10000;
        let replicas = 3;

        let racks = 3;
        let hosts = 3;
        let osds = 10;

        let c = build_datacenter_cluster(racks, hosts, osds);

        let mut count = HashMap::new();

        for pg in 1..=num_of_pgs {
            let racks = c.select(pg, replicas, "");
            let mut placement = vec![];

            for rack in racks {
                let hosts = c.select(pg, 1, &rack);
                for host in hosts {
                    let osds = c.select(pg, 1, &format!("{}/{}", rack, host));
                    placement.push(format!("{}/{}/{}", rack, host, osds[0]))
                }
            }

            // put all the pg placements into a hashmap for counting spread
            for p in &placement {
                if let Some(x) = count.get_mut(p) {
                    *x += 1;
                } else {
                    count.insert(p.clone(), 1);
                }
            }

            // validate each pg is in three unique racks
            let set: HashSet<String> = placement
                .iter()
                .map(|x| x.split_once("/").unwrap().0.to_string())
                .collect();
            assert!(set.len() == 3);
        }

        let exact_percentage = 100.0 / (racks * hosts * osds) as f64;

        // make sure the actual is within a percent of the theoretical
        for (_, c) in count {
            let actual_percentage = c as f64 / (num_of_pgs * replicas) as f64 * 100.0;
            assert!(actual_percentage - exact_percentage < 1.0);
        }
    }

    // /// test distribute on insert
    // #[test]
    // fn move_factor_add() {
    //     let mut crush = gen_test_map();
    //     let crush0 = crush.clone();
    //     let mut rng = rand::thread_rng();

    //     // random choose 10 OSDs, add weight to them
    //     for _ in 0..10 {
    //         let i = rng.gen_range(0..9);
    //         let j = rng.gen_range(0..9);
    //         let k = rng.gen_range(0..9);
    //         let l = rng.gen_range(0..10);
    //         let path = path_from_nums(i, j, k, l);
    //         let weight = rng.gen_range(1..5);
    //         crush.add_weight(&path, weight as i64);
    //     }

    //     let n = 1000000;
    //     let move_count = (0..n)
    //         .filter(|&i| crush0.locate(i) != crush.locate(i))
    //         .count();
    //     let shift_weight = crush.total_weight() - crush0.total_weight();
    //     let move_fator =
    //         (move_count as f32) / (n as f32 / (crush0.total_weight() / shift_weight) as f32);
    //     assert!(move_fator < 4.0, "move factor {move_fator} should < 4");
    // }

    // /// test distribute on remove
    // #[test]
    // fn move_factor_remove() {
    //     let mut crush = gen_test_map();
    //     let crush0 = crush.clone();
    //     let mut rng = rand::thread_rng();

    //     // shut down around 90 osds
    //     let mut shift_weight = 0;
    //     for _ in 0..90 {
    //         let i = rng.gen_range(0..9);
    //         let j = rng.gen_range(0..9);
    //         let k = rng.gen_range(0..9);
    //         let l = rng.gen_range(0..10);
    //         let path = path_from_nums(i, j, k, l);
    //         if !crush.get_inout(&path) {
    //             crush.set_inout(&path, true);
    //             shift_weight += crush.get_weight(&path);
    //         }
    //     }

    //     let n = 1000000;
    //     let move_count = (0..n)
    //         .filter(|&i| crush0.locate(i) != crush.locate(i))
    //         .count();
    //     let move_factor =
    //         (move_count as f32) / (n as f32 / (crush0.total_weight() / shift_weight) as f32);
    //     assert!(move_factor < 1.5, "move factor {move_factor} should < 1.5");
    // }
}
