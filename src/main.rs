use smol_lsm::LSMTree;

fn main() {
    let mut tree = LSMTree::new(2);
    tree.insert(b"hello".to_vec(), b"world".to_vec());
    let value = tree.get(b"hello");
    println!("{:?}", String::from_utf8(value.unwrap()).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main() {}
}
