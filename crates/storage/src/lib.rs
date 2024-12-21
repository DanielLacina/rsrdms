use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::{Read, Result, Write};

const PAGE_SIZE: usize = 8192;

struct HeaderOffsets {
    pub lsn: (usize, usize),
    pub checksum: (usize, usize),
    pub flags: (usize, usize),
    pub lower: (usize, usize),
    pub higher: (usize, usize),
    pub special_space: (usize, usize),
}

struct Storage {
    header_offsets: HeaderOffsets,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            header_offsets: HeaderOffsets {
                lsn: (0, 8),
                checksum: (8, 10),
                flags: (10, 12),
                lower: (12, 14),
                higher: (14, 16),
                special_space: (16, 18),
            },
        }
    }

    pub fn read_postgres_file(&self, file_path: &str) -> Result<Vec<(u8, String)>> {
        let mut file = File::open(file_path)?;
        let mut page = [0u8; PAGE_SIZE];

        // Read the entire page into memory
        file.read_exact(&mut page)?;

        // Parse header
        let lsn = u64::from_le_bytes(
            page[self.header_offsets.lsn.0..self.header_offsets.lsn.1]
                .try_into()
                .unwrap(),
        );
        let checksum = u16::from_le_bytes(
            page[self.header_offsets.checksum.0..self.header_offsets.checksum.1]
                .try_into()
                .unwrap(),
        );
        let flags = u16::from_le_bytes(
            page[self.header_offsets.flags.0..self.header_offsets.flags.1]
                .try_into()
                .unwrap(),
        );
        let lower = u16::from_le_bytes(
            page[self.header_offsets.lower.0..self.header_offsets.lower.1]
                .try_into()
                .unwrap(),
        );
        let higher = u16::from_le_bytes(
            page[self.header_offsets.higher.0..self.header_offsets.higher.1]
                .try_into()
                .unwrap(),
        );
        let special_space = u16::from_le_bytes(
            page[self.header_offsets.special_space.0..self.header_offsets.special_space.1]
                .try_into()
                .unwrap(),
        );

        println!("Header:");
        println!("  LSN: {}", lsn);
        println!("  Checksum: {}", checksum);
        println!("  Flags: {}", flags);
        println!("  Lower Pointer: {}", lower);
        println!("  Higher Pointer: {}", higher);
        println!("  Special Space: {}", special_space);

        let mut pointers = Vec::new();
        let mut offset = 18;
        while offset < lower {
            let offset_index = offset as usize;
            let pointer =
                u16::from_le_bytes(page[offset_index..(offset_index + 2)].try_into().unwrap());
            pointers.push(pointer as usize);
            offset += 2;
        }

        let mut values = Vec::new();

        for pointer in pointers {
            let (id_start, id_end) = (pointer, pointer + 1);
            let id = u8::from_le_bytes(page[id_start..id_end].try_into().unwrap());
            let (name_length_start, name_length_end) = (id_end, id_end + 2);
            let name_length =
                u16::from_le_bytes(page[name_length_start..name_length_end].try_into().unwrap());
            let name = String::from_utf8(
                page[name_length_end..name_length_end + name_length as usize]
                    .try_into()
                    .unwrap(),
            )
            .unwrap();
            values.push((id, name));
        }
        Ok(values)
    }

    pub fn insert_data(&self, file_path: &str, data: &Vec<(u8, String)>) -> Result<()> {
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let mut page = [0u8; PAGE_SIZE];
        file.read_exact(&mut page)?;
        let (lower_offset_start, lower_offset_end) =
            (self.header_offsets.lower.0, self.header_offsets.lower.1);
        let mut lower = u16::from_le_bytes(
            page[lower_offset_start..lower_offset_end]
                .try_into()
                .unwrap(),
        );
        let (higher_offset_start, higher_offset_end) =
            (self.header_offsets.higher.0, self.header_offsets.higher.1);
        let mut higher = u16::from_le_bytes(
            page[higher_offset_start..higher_offset_end]
                .try_into()
                .unwrap(),
        );
        for (id, name) in data {
            let id_bytes = id.to_le_bytes();
            let name_bytes = name.as_bytes();
            let name_length_bytes = (name_bytes.len() as u16).to_le_bytes();
            let tuple_size: u16 = (id_bytes.len() + 2 + name_bytes.len()) as u16;
            higher -= tuple_size;
            let (pointer_start, pointer_end) = (lower as usize, lower as usize + 2);
            page[pointer_start..pointer_end].copy_from_slice(&(higher).to_le_bytes());
            // modifies lower and upper bounds
            lower = pointer_end as u16;
            //
            let (id_start, id_end) = (higher as usize, higher as usize + id_bytes.len());
            page[id_start..id_end].copy_from_slice(&id_bytes);
            let (name_length_start, name_length_end) = (id_end, id_end + name_length_bytes.len());
            page[name_length_start..name_length_end].copy_from_slice(&name_length_bytes);
            let (name_start, name_end) = (name_length_end, name_length_end + name_bytes.len());
            page[name_start..name_end].copy_from_slice(&name_bytes);
        }
        page[lower_offset_start..lower_offset_end].copy_from_slice(&lower.to_le_bytes());
        page[higher_offset_start..higher_offset_end].copy_from_slice(&higher.to_le_bytes());
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page)?;
        Ok(())
    }

    pub fn create_postgres_file(&self, file_path: &str) -> Result<()> {
        let mut file = File::create(file_path)?;
        let mut page = [0u8; PAGE_SIZE];

        let lsn: u64 = 12345678;
        page[0..8].copy_from_slice(&lsn.to_le_bytes());
        let checksum: u16 = 42;
        page[8..10].copy_from_slice(&checksum.to_le_bytes());
        let flags: u16 = 40;
        page[10..12].copy_from_slice(&flags.to_le_bytes());
        let lower = 18;
        page[12..14].copy_from_slice(&(lower as u16).to_le_bytes());
        let higher = PAGE_SIZE as u16;
        page[14..16].copy_from_slice(&higher.to_le_bytes());
        let special_space: u16 = PAGE_SIZE as u16;
        page[16..18].copy_from_slice(&special_space.to_le_bytes());
        file.write_all(&page)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_postgres_file() {
        let storage = Storage::new();
        let file_path = "src/base/hello".to_string();
        storage.create_postgres_file(&file_path).unwrap();
        let data = vec![(1, "bob".to_string()), (2, "bill".to_string())];
        storage.insert_data(&file_path, &data).unwrap();
        let values = storage.read_postgres_file(&file_path).unwrap();
        assert_eq!(values, data);
    }
}
