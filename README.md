# crush

An implementation of Ceph's [CRUSH](https://www.ssrc.ucsc.edu/media/pubs/9c7bcd06ff4eeccef2cb4c7813fe33ba7d4805c7.pdf) in Rust.

The goal of this repo is to make a fairly simple, working implemention of CRUSH for people to reference.

Unit tests will exercise the various features of CRUSH.

## Details

The foundation for this comes from https://github.com/madsys-dev/crush. Without this, I would have had a much harder time.

It had a few problems I wanted to solve for my project:

1. Non-deterministic hashing. It uses [aHash](https://github.com/tkaitchuck/aHash) which was giving me different results between runs. This might be tunable, but the philosophy of aHash not having a fixed standard didn't seem like a good fit for building a CRUSH map. For now I've settled on `DefaultHasher` from the Rust standard library, as both implement the `Hasher` trait.
2. `select()` automatically dropping down to the OSD level. I wanted to make my implemention of CRUSH support a failure domain at any level in the map. I changed the `select()` logic to be more in-line with the CRUSH paper. It returns a `vec` of `String` for the level it chooses.

It has a few things I don't quite understand yet:

1. `choose()` - I'm not sure if what is written lines up with any of the bucket types listed in the Ceph paper. Granted, since this version of `choose()` worked so well with deterministic hashing, I barely touched it.

## TODO

- More unit tests
- Understand `choose()` more and possibly replace with implementations of all the buckets from the CRUSH paper
