# v4 upgrade Note

## Version Changes
- DataHeader version: 0x02
- BlobHeader version: 0x02
- Shard superblock version: 0x02

Currently only support version backward compatibility.

## Incompatibilities and Breaking Changes

### 1. Fixed-Size BlobHeader (4KB)
- Add a blob_header_version=0x02 
- Move user key into the BlobHeader. Note, currently gw doesn't pass user key, but in next version, it will put object identifier as user key.
- More padding to align BlobHeader to 4KB

### 2. Shard Metadata Persistence
- Add a shard sb_verison=0x02
- Add metadata field to ShardInfo structure for future use

### 3. Partial Read Optimization
**Impact:** New capability, backward compatible for new reads but changes read behavior
- Added `allow_skip_verify` parameter to enable optimized partial reads
- When enabled partial read, skips header read and directly reads requested data range
- Improves HDD performance by reducing I/O operations
- Only works with v4's fixed-size header (predictable data offset)

## Upgrade Plan
1. **Stop PG traffic** to all HomeObject instances
2. **Rewrite all existing data** and upgrade to v4 in place using offline refactor tool:
   - Blob data
     - Read each blob with v3 format
     - Rewrite with v4 fixed-size BlobHeader format
     - Embed user_key into BlobHeader (validate max 1024 bytes)
     - Update data_offset to fixed value (4096)
     - Recalculate hash without separate user_key parameter
   - Update shard metadata
     - Rewrite shard superblock with version 0x02
     - Rewrite shard header and footer in chunks
3. **If chunk space is insufficient**, migrate the data from upper layer(rclone nuobject data)
4. **Restore service and traffic**