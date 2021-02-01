#[cfg(test)]
mod tests {
	#[test]
	fn it_works() {
		assert_eq!(2 + 2, 4);
	}
}

mod dag;
mod nodes;
pub mod skeleton;
mod testing;
mod traits;
mod creator;
