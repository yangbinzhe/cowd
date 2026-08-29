use super::{duplicate_authority, has_flag, Roots};

pub(super) fn run(roots: &Roots, arguments: &[String]) -> Result<(), String> {
    let count = duplicate_authority::validate_duplicate_policy(roots)?;
    if has_flag(arguments, "--check") {
        println!("duplicate-capability gate passed: classified_candidates={count}");
    }
    Ok(())
}
