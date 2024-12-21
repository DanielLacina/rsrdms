use std::fs::{File, OpenOptions};
use std::io::Seek;
use std::io::{Read, Result, Write};

const PAGE_SIZE: usize = 8192;

#[derive(Debug, Clone)]
pub enum DataType {
    String(String),
    Integer32(i32),
    Float32(f32),
}

#[derive(Debug, PartialEq)]
struct TableMetadata {
    pub table_id: u32,
    pub table_name: String,
    pub data_file_path: String,
}

#[derive(Debug)]
struct ColumnMetadata {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

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

    pub fn read_postgres_class(&self, file_path: &str) -> Result<Vec<TableMetadata>> {
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let mut page = [0u8; PAGE_SIZE];
        file.read_exact(&mut page)?; // Parse header
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

        let mut pointers = Vec::new();
        let mut offset = 18;
        while offset < lower {
            let offset_index = offset as usize;
            let pointer =
                u16::from_le_bytes(page[offset_index..(offset_index + 2)].try_into().unwrap());
            pointers.push(pointer as usize);
            offset += 2;
        }

        let mut tables = Vec::new();
        for pointer in pointers {
            let (start, end) = (pointer, pointer + 4);
            let table_id = u32::from_le_bytes(page[start..end].try_into().unwrap());
            let (start, end) = (end, end + 2);
            let table_name_length = u16::from_le_bytes(page[start..end].try_into().unwrap());
            let (start, end) = (end, end + table_name_length as usize);
            let table_name = String::from_utf8(page[start..end].to_vec()).unwrap();
            let (start, end) = (end, end + 2);
            let data_file_path_length = u16::from_le_bytes(page[start..end].try_into().unwrap());
            let (start, end) = (end, end + data_file_path_length as usize);
            let data_file_path = String::from_utf8(page[start..end].to_vec()).unwrap();
            tables.push(TableMetadata {
                table_id,
                table_name,
                data_file_path,
            });
        }
        println!("{:?}", tables);
        Ok(tables)
    }

    pub fn write_postgres_class(
        &self,
        file_path: &str,
        tables_metadata: &Vec<TableMetadata>,
    ) -> Result<()> {
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

        for table_metadata in tables_metadata {
            let id_bytes = table_metadata.table_id.to_le_bytes();
            let table_name_bytes = table_metadata.table_name.as_bytes();
            let table_name_length_bytes = (table_name_bytes.len() as u16).to_le_bytes();
            let data_file_path_bytes = table_metadata.data_file_path.as_bytes();
            let data_file_path_length_bytes = (data_file_path_bytes.len() as u16).to_le_bytes();

            let data_length = id_bytes.len()
                + table_name_bytes.len()
                + table_name_length_bytes.len()
                + data_file_path_bytes.len()
                + data_file_path_length_bytes.len();
            higher -= data_length as u16;
            page[lower as usize..lower as usize + 2].copy_from_slice(&higher.to_le_bytes());
            lower += 2;
            let (start, end) = (higher as usize, higher as usize + 4);
            page[start..end].copy_from_slice(&id_bytes);
            let (start, end) = (end, end + 2);
            page[start..end].copy_from_slice(&table_name_length_bytes);
            let (start, end) = (end, end + table_name_bytes.len());
            page[start..end].copy_from_slice(&table_name_bytes);
            let (start, end) = (end, end + 2);
            page[start..end].copy_from_slice(&data_file_path_length_bytes);
            let (start, end) = (end, end + data_file_path_bytes.len());
            page[start..end].copy_from_slice(&data_file_path_bytes);
        }
        page[higher_offset_start..higher_offset_end].copy_from_slice(&higher.to_le_bytes());
        page[lower_offset_start..lower_offset_end].copy_from_slice(&lower.to_le_bytes());
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page)?;
        Ok(())
    }

    pub fn create_postgres_class(&self, file_path: &str) -> Result<()> {
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
    fn test_write_postgres_class() {
        let storage = Storage::new();
        let file_path = "src/base/table".to_string();
        let tables_metadata = vec![
            TableMetadata {
                table_id: 1,
                table_name: "accounts".to_string(),
                data_file_path: "src/base/accounts".to_string(),
            },
            TableMetadata {
                table_id: 2,
                table_name: "users".to_string(),
                data_file_path: "src/base/users".to_string(),
            },
            TableMetadata {
                table_id: 3,
                table_name: "orders".to_string(),
                data_file_path: "src/base/orders".to_string(),
            },
            TableMetadata {
                table_id: 4,
                table_name: "products".to_string(),
                data_file_path: "src/base/products".to_string(),
            },
            TableMetadata {
                table_id: 5,
                table_name: "transactions".to_string(),
                data_file_path: "src/base/transactions".to_string(),
            },
        ];
        storage.create_postgres_class(&file_path);
        storage.write_postgres_class(&file_path, &tables_metadata);
        let read_tables_metadata = storage.read_postgres_class(&file_path).unwrap();
        assert_eq!(read_tables_metadata, tables_metadata);
    }
}
