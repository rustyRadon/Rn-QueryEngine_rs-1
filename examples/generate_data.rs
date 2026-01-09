use std::fs::File;
use std::io::{BufWriter,};
use byteorder::{LittleEndian, WriteBytesExt};

fn main() {
    let count = 1_000_000;
    
    let mut age_file = BufWriter::new(File::create("age.bin").unwrap());
    
    let mut salary_file = BufWriter::new(File::create("salary.bin").unwrap());

    for i in 0..count {
        let age = (i % 43) + 18; 
        let salary = age * 2000;
        
        age_file.write_i32::<LittleEndian>(age).unwrap();
        salary_file.write_i32::<LittleEndian>(salary).unwrap();
    }
    println!("Successfully generated {} rows in age.bin and salary.bin", count);
}

//cargo run --example generate_data
//time cargo run -- "SELECT age, salary WHERE age > 21" > /dev/null