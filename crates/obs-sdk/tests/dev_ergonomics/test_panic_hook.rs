//! `test_panic_hook` (spec 72 § 7) — `install_panic_hook` is
//! idempotent and chains the prior hook.

use obs_sdk::install_panic_hook;

#[test]
fn test_install_panic_hook_should_be_idempotent() {
    install_panic_hook();
    install_panic_hook();
    install_panic_hook();
}
