extern crate alloc;

use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::hash::{Hash, Hasher};
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

// https://www.techiedelight.com/round-next-highest-power-2/
fn find_next_power_of_2(n: u32) -> u32 {
    let mut n = n;
    n -= 1;
    while n & n - 1 != 0 {
        n = n & n - 1;
    }
    n << 1
}

impl Crush {
    /// Add weight to a node.
    pub fn add_weight(&mut self, path: &str, weight: i64) {
        self.root.add_weight(path, weight);
    }

    /// Locate a node by `pgid`.
    pub fn locate(&self, pgid: u32) -> String {
        let mut path = String::new();
        while !path.contains("osd") {
            let select = self.select(pgid, 1, &path).into_iter().next().unwrap();
            if path == "" {
                path = select;
            } else {
                path = format!("{}/{}", path, select);
            }
        }
        path
    }

    pub fn get_osds(&self, node: &Node) -> u32 {
        // returns the number of osds from the given node
        let mut count = 0;
        for (name, child) in &node.children {
            if name.contains("osd") {
                count += 1;
            } else {
                count += self.get_osds(child);
            }
        }
        count
    }

    // https://docs.ceph.com/en/latest/rados/operations/placement-groups/#choosing-the-number-of-placement-groups
    pub fn get_recommended_pgs(&self, replicas: u32) -> u32 {
        find_next_power_of_2(self.get_osds(&self.root) * 100 / replicas)
    }

    pub fn locate_all(&self, pgid: u32, replicas: u32) -> Vec<String> {
        let mut paths = self.select(pgid, replicas, "");
        for p in paths.iter_mut() {
            while !p.contains("osd") {
                let select = self.select(pgid, 1, p);
                for s in select {
                    *p = format!("{}/{}", p, s);
                }
            }
        }
        paths.sort();
        paths
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
    use super::*;
    use alloc::format;
    use std::collections::{HashMap, HashSet};

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
    fn single_diverse_and_spread() {
        let num_of_pgs = 16_384;
        let replicas = 3;

        let osds = 5;

        let c = build_single_node_cluster(osds);
        let mut count = HashMap::new();

        for pg in 1..=num_of_pgs {
            let osds = c.select(pg, replicas, "");
            let mut placement = vec![];
            for osd in osds {
                placement.push(osd);
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
            let set: HashSet<String> = placement.into_iter().collect();
            assert!(set.len() == 3);
        }

        let exact_percentage = 100.0 / osds as f64;

        // make sure the actual is within a percent of the theoretical
        for (_, c) in count {
            let actual_percentage = c as f64 / (num_of_pgs * replicas) as f64 * 100.0;
            assert!(actual_percentage - exact_percentage < 1.0);
        }
    }

    #[test]
    fn ha_diverse_and_spread() {
        // simulate a replicas: 3 cluster with a failure domain of "host" in a homelab environment
        let num_of_pgs = 16_384;
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
        let num_of_pgs = 16_384;
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

    #[test]
    fn move_factor_add() {
        // a 3-node cluster with 5 disks per node
        // gets a new server provisioned because the company needs more storage
        // the final topology is a 4-node cluster with 5 disks per node

        let hosts = 3;
        let osds = 5;

        let num_of_pgs = 16_384;
        let replicas = 3;

        let before_crush = build_ha_cluster(hosts, osds);
        let mut after_crush = before_crush.clone();

        // add in fourth node to the "after" crush map

        for i in 1..=osds {
            after_crush.add_weight(&format!("host.4/osd.{}", i), 1);
        }

        let mut moved = 0;
        for pg in 1..=num_of_pgs {
            let before_locate = before_crush.locate_all(pg, replicas);
            let after_locate = after_crush.locate_all(pg, replicas);

            if before_locate != after_locate {
                println!("before: {:?}, after: {:?}", before_locate, after_locate);
                moved += 1;
            }
        }
        let moved_percentage = moved as f64 / num_of_pgs as f64 * 100.0;
        println!(
            "moved: {} out of {} pgs, which is {}%",
            moved, num_of_pgs, moved_percentage
        );
        // for now, make sure we move less than 80% of data within a placement group
        // when adding a fourth node to a previously 3 node 3 replica cluster, the % of pg's affected is fairly significant
        assert!(moved_percentage < 80.0);
    }

    #[test]
    fn recommended_pgs() {
        // does the recommended number of PGs match the example formula in the ceph documentation?
        // https://docs.ceph.com/en/latest/rados/operations/placement-groups/#choosing-the-number-of-placement-groups

        // total of 200 OSDs
        let racks = 5;
        let hosts = 5;
        let osds = 8;

        let crush = build_datacenter_cluster(racks, hosts, osds);

        assert_eq!(crush.get_recommended_pgs(3), 8192);
    }
}
