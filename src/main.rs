mod crush;

use std::collections::HashMap;

use crate::crush::crush::Crush;

fn main() {
    let mut c = Crush::default();

    for rack in 1..=3 {
        for host in 1..=3 {
            for osd in 1..=3 {
                let path = format!("rack.{}/host.{}/osd.{}", rack, host, osd);
                c.add_weight(&path, 1);
                println!("Added {}", &path);
            }
        }
    }

    // simulate the placement of n pgs with replica: 3
    let num_of_pgs = 10;
    let replicas = 3;

    let mut count: HashMap<String, u32> = HashMap::<String, u32>::new();
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

        println!("pg {} will be placed on: {:?}", pg, placement);
    }

    // print the spread
    let mut total_pgs = 0;
    let mut keys: Vec<&String> = count.keys().collect();
    keys.sort();
    for location in keys {
        let value = count.get(location).unwrap();
        println!(
            "pgs in location {}: {}, which is {:.2}%",
            location,
            value,
            *value as f64 / (num_of_pgs as f64 * replicas as f64) * 100.0
        );
        total_pgs += value;
    }

    println!("total pgs: {}", total_pgs);
}
