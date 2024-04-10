# TimeSeriesFiles

DRAFT(v1)

file extension tsf

File is built to be extremely fast inserting data, utilizing append data structures.
Use uuidv7 to uniquely identify segments in a timestamp fasion

Indexes and offset will be stored in a separate file. In case of indexes or offsets get corrupted. The file format should be able to recreate the indexes and offsets by doing a full scan.

## File Header

Uniquely Identifies file and version

+-----u32------+---u16---+
| Magic Number | Version |
+--------------+---------+

* Magic Number - Makes sure we are reading a tsf file
* Version - Tells us what version the file is, backwards compatibility is not guaranteed and new versions might require a full copy to the new version.

## Segments

The file can have multiple segments appended to it. This aids in the ability to append new data in a fast manner. Segments are designed to be immutable. There are 3 types of segments:
1. Segment Data - contains all the raw data
2. Segment Delete - contains unique segment rows marked for deletion
3. Segment Update - contains unique segment rows where values have been updated

### Segment Data

#### Segment Data Header

The Segment header gives metadata and how to read the data

+---u8--+-----u32-----+---u8x16---+-----i64----+----i64---+----u32----+------u16-----+----u16----+--------u32---------+------(n)-------+------u8x8-----+
| state | next_offset | uuid_txid | date_start | date_end | row_count | column_count | ts_column | column_header_size | column_headers | segment_check |
+-------+-------------+-----------+------------+----------+-----------+--------------+-----------+--------------------+----------------+---------------+

* state - Various state the segment can be in
  * Active - Current segment available for reading
  * Creating - Writing to file currently
  * Deleted - Marked for removal
* next_offset - amount of bytes to next segment header
* uuid_txid - uuidv7 timestamp of the start of a transaction to make it unique against the file.
* date_start - UTS of the start range of data
* date_end - UTS of the end range of data
* row_count - tells us the number of rows in the data
* column_count - tells us the number of colums in the data
* ts_column - indicates which column is the dedicated timeseries
* column_header_size - tells us the size in bytes of column headers
* column_headers - is another struct to read metadata about individual columns
* segment_check - 64 bits of the XXH64 integrity check of the segment header

##### Column Header

The column header tells us information about specific columns

+---------u16--------+-----len-----+-----u16-----+--------u16---------+-----len-----+-----u8-----+-----u8------+-----u64-----+-----b64------+
| column_name_length | column_name | column_type | column_meta_length | column_meta | column_enc | column_comp | column_size | column_check |
+--------------------+-------------+-------------+--------------------+-------------+------------+-------------+-------------+--------------+

* column_name_length - Length of the column name
* column_name - string with the name of the column, length provide with previous
* column_type - u16 enum of the column type
* column_meta_length - u16 of the length of column_meta, if 0 there is no metadata
* column_meta - variable type depening on the column_type
* column_enc - u8 enum of the type of encoding
* column_comp - u8 enum type of compression
* column_size - u64 of the total size of the column data
* column_check - b64 of the XXH64 integrity check of data

#### Column Data

Data is stored as columns, since this is the most efficient way to store specific data which we can encode and compresses better.

Data is stored per column and the size of the data after encoding and compression is stored in the column header metadata.

+----------+----------+----------+
| column_1 | column_2 | column_n |
+----------+----------+----------+

### Segment Delete

Segment Delete is designed to mark specific rows within a data segment as deleted without physically removing the data. This segment type aids in logically removing data while maintaining the integrity and immutability of the original data segments.

#### Segment Delete Header

Similar to the Segment Data Header but tailored for deletion metadata.

+---u8x16---+----u64----+------b64------+
| uuid_txid | del_count | segment_check |
+-----------+-----------+---------------+

* uuid_txid - Defines the associated txid to remove records from query.
* del_count - The number of rows marked for deletion within the specified range.
* segment_check - b64 of the XXH64 integrity check of the segment delete header.

#### Segement Delete Records

Each record specifies a row, or range of rows, to be deleted.

+-------u64-------+-------u64-------+
| start_row_index | end_row_index   |
+-----------------+-----------------+

* start_row_index - The starting index of a row or range of rows to be deleted.
* end_row_index - The ending index of a row or range of rows to be deleted. For single row deletions, this would be equal to start_row_index.


### Segment Update

Segment Update contains updates to specific rows within a data segment. This segment type enables modifying data in an immutable manner by appending new versions of rows.

#### Segment Update Header

Follows the Segment Data Header structure, with modifications to indicate it's an update segment.

+---u8x16---+----u64----+----u64----+------(n)-------+------b64------+
| uuid_txid | upd_count | col_count | column_headers | segment_check |
+-----------+-----------+-----------+----------------+---------------+

* uuid_txid - Defines the associated txid to update rows in the query with
* upd_count - The number of rows being updated.
* col_count - The number of columns in the update data, which may be fewer than in the original segment.
* column_headers - Metadata about the columns being updated.
* segment_check - b64 of the XXH64 integrity check of the segment update header.

#### Segment Update Records

The structure of update records mirrors the column data storage in Segment Data, with each record corresponding to an updated row.

+----u64----+----------+----------+----------+
| row_index | column_1 | column_2 | column_n |
+-----------+----------+----------+----------+

* row_index - Specifies the index of the row in the original segment that is being updated.
* column_n - Contains the updated data for each column. Columns not present in the update are assumed to retain their original values.
