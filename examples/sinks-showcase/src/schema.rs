//! Wire `obs-build`'s codegen into the binary. The macro pulls in
//! generated wire types (`buffa-build`) and the obs schema registry
//! plumbing (`obs-build`) for every annotated message in `showcase.v1`.

obs_sdk::include_schemas!("showcase.v1");
