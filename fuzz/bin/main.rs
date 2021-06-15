use aleph_bft::testing::fuzz;
use std::{
    io,
    io::{BufReader, BufWriter},
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "fuzz-helper",
    about = "data generator for the purpose of fuzzing"
)]
struct Opt {
    /// Verify data provided on stdin by calling member::run on it.
    #[structopt(short, long)]
    check_fuzz: bool,

    /// Generate data for a given number of members.
    /// When used with the 'check_fuzz' flag it verifies data assuming this number of members.
    #[structopt(default_value = "4")]
    members: usize,

    /// Generate a given number of batches.
    /// When used with the 'check_fuzz' flag it will verify if we are able to create at least this number of batches.
    #[structopt(default_value = "30")]
    batches: usize,
}

fn main() {
    let opt = Opt::from_args();
    if opt.check_fuzz {
        fuzz::check_fuzz(BufReader::new(io::stdin()), opt.members, Some(opt.batches));
    } else {
        fuzz::generate_fuzz(BufWriter::new(io::stdout()), opt.members, opt.batches);
    }
}
