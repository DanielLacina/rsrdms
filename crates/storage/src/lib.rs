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
}

#[derive(Debug, PartialEq)]
struct ColumnMetadata {
    column_id: u32,
    table_id: u32,
    column_name: String,
    data_type: String,
    is_nullable: bool,
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

    pub fn read_metadata<F, T>(&self, file_path: &str, parse_entry: F) -> Result<Vec<T>>
    where
        F: Fn(&[u8], usize) -> (T, usize),
    {
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
        let mut page = [0u8; PAGE_SIZE];
        file.read_exact(&mut page)?;

        let lower = u16::from_le_bytes(
            page[self.header_offsets.lower.0..self.header_offsets.lower.1]
                .try_into()
                .unwrap(),
        );

        let mut pointers = Vec::new();
        let mut offset = 18; // Start of the directory
        while offset < lower {
            let pointer = u16::from_le_bytes(
                page[offset as usize..offset as usize + 2]
                    .try_into()
                    .unwrap(),
            );
            pointers.push(pointer as usize);
            offset += 2;
        }

        let mut entries = Vec::new();
        for pointer in pointers {
            let (entry, _) = parse_entry(&page, pointer);
            entries.push(entry);
        }
        Ok(entries)
    }

    pub fn write_metadata<F>(
        &self,
        file_path: &str,
        entries: Vec<Vec<u8>>,
        calculate_size: F,
    ) -> Result<()>
    where
        F: Fn(&[u8]) -> usize,
    {
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

        for entry in entries {
            let entry_size = calculate_size(&entry);

            if higher < entry_size as u16 || (lower as usize + 2) > PAGE_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Insufficient space in page.",
                ));
            }

            higher -= entry_size as u16;
            page[lower as usize..lower as usize + 2].copy_from_slice(&higher.to_le_bytes());
            lower += 2;

            page[higher as usize..higher as usize + entry_size].copy_from_slice(&entry);
        }

        page[higher_offset_start..higher_offset_end].copy_from_slice(&higher.to_le_bytes());
        page[lower_offset_start..lower_offset_end].copy_from_slice(&lower.to_le_bytes());
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page)?;

        Ok(())
    }

    pub fn read_postgres_class(&self, file_path: &str) -> Result<Vec<TableMetadata>> {
        self.read_metadata(file_path, |page, pointer| {
            let mut offset = pointer;

            let table_id = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let table_name_length =
                u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
            offset += 2;

            let table_name =
                String::from_utf8(page[offset..offset + table_name_length as usize].to_vec())
                    .unwrap();
            offset += table_name_length as usize;

            (
                TableMetadata {
                    table_id,
                    table_name,
                },
                offset,
            )
        })
    }

    pub fn write_postgres_class(
        &self,
        file_path: &str,
        tables_metadata: &Vec<TableMetadata>,
    ) -> Result<()> {
        let entries: Vec<Vec<u8>> = tables_metadata
            .iter()
            .map(|table| {
                let mut data = vec![];
                data.extend_from_slice(&table.table_id.to_le_bytes());
                data.extend_from_slice(&(table.table_name.len() as u16).to_le_bytes());
                data.extend_from_slice(table.table_name.as_bytes());
                data
            })
            .collect();

        self.write_metadata(file_path, entries, |entry| entry.len())
    }

    pub fn read_postgres_attribute(&self, file_path: &str) -> Result<Vec<ColumnMetadata>> {
        self.read_metadata(file_path, |page, pointer| {
            let mut offset = pointer;

            let column_id = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let table_id = u32::from_le_bytes(page[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let column_name_length =
                u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
            offset += 2;

            let column_name =
                String::from_utf8(page[offset..offset + column_name_length as usize].to_vec())
                    .unwrap();
            offset += column_name_length as usize;

            let data_type_length = u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
            offset += 2;

            let data_type =
                String::from_utf8(page[offset..offset + data_type_length as usize].to_vec())
                    .unwrap();
            offset += data_type_length as usize;

            let is_nullable = page[offset] != 0;
            offset += 1;

            (
                ColumnMetadata {
                    column_id,
                    table_id,
                    column_name,
                    data_type,
                    is_nullable,
                },
                offset,
            )
        })
    }

    pub fn write_postgres_attribute(
        &self,
        file_path: &str,
        columns_metadata: &Vec<ColumnMetadata>,
    ) -> Result<()> {
        let entries: Vec<Vec<u8>> = columns_metadata
            .iter()
            .map(|column| {
                let mut data = vec![];
                data.extend_from_slice(&column.column_id.to_le_bytes());
                data.extend_from_slice(&column.table_id.to_le_bytes());
                data.extend_from_slice(&(column.column_name.len() as u16).to_le_bytes());
                data.extend_from_slice(column.column_name.as_bytes());
                data.extend_from_slice(&(column.data_type.len() as u16).to_le_bytes());
                data.extend_from_slice(column.data_type.as_bytes());
                data.push(if column.is_nullable { 1 } else { 0 });
                data
            })
            .collect();

        self.write_metadata(file_path, entries, |entry| entry.len())
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
    fn test_write_postgres_class() {
        let storage = Storage::new();
        let file_path = "src/base/table".to_string();
        let columns_metadata = vec![
            ColumnMetadata {
                column_id: 1,
                table_id: 42,
                column_name: "user_id".to_string(),
                data_type: "INTEGER".to_string(),
                is_nullable: false,
            },
            ColumnMetadata {
                column_id: 2,
                table_id: 42,
                column_name: "username".to_string(),
                data_type: "VARCHAR".to_string(),
                is_nullable: false,
            },
            ColumnMetadata {
                column_id: 3,
                table_id: 42,
                column_name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                is_nullable: true,
            },
            ColumnMetadata {
                column_id: 4,
                table_id: 42,
                column_name: "created_at".to_string(),
                data_type: "TIMESTAMP".to_string(),
                is_nullable: false,
            },
        ];
        let tables_metadata = vec![
            TableMetadata {
                table_id: 1,
                table_name: "accounts".to_string(),
            },
            TableMetadata {
                table_id: 2,
                table_name: "users".to_string(),
            },
            TableMetadata {
                table_id: 3,
                table_name: "orders".to_string(),
            },
            TableMetadata {
                table_id: 4,
                table_name: "products".to_string(),
            },
            TableMetadata {
                table_id: 5,
                table_name: "transactions".to_string(),
            },
        ];
        storage.create_postgres_file(&file_path).unwrap();
        storage
            .write_postgres_class(&file_path, &tables_metadata)
            .unwrap();
        let read_tables_metadata = storage.read_postgres_class(&file_path).unwrap();
        assert_eq!(read_tables_metadata, tables_metadata);
        let file_path = "src/base/column".to_string();
        storage.create_postgres_file(&file_path).unwrap();
        storage.write_postgres_attribute(&file_path, &columns_metadata);
        let read_columns_metadata = storage.read_postgres_attribute(&file_path).unwrap();
        assert_eq!(read_columns_metadata, columns_metadata);
    }
}
