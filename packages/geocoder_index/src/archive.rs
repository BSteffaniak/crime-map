//! Tar + zstd archive utilities for syncing the Tantivy index to R2.
//!
//! The Tantivy index is a directory of segment files. Since R2 (and
//! the existing sync mechanism) operates on single flat files, we
//! pack the directory into a `.tar.zst` archive for upload and unpack
//! it on download.

use std::path::Path;

/// Default zstd compression level. Level 3 gives a good balance of
/// speed and compression ratio.
const ZSTD_LEVEL: i32 = 3;

/// Packs a directory into a `.tar.zst` archive.
///
/// The archive is written atomically (to a temp file first, then
/// renamed) to avoid leaving a corrupt archive if interrupted.
///
/// # Errors
///
/// Returns an I/O error if the directory cannot be read or the
/// archive cannot be written.
pub fn pack(dir: &Path, archive_path: &Path) -> Result<u64, ArchiveError> {
    if !dir.exists() {
        return Err(ArchiveError::DirectoryNotFound(dir.display().to_string()));
    }

    log::info!("Packing {} -> {}", dir.display(), archive_path.display());

    // Ensure parent directory exists
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| ArchiveError::Io {
            path: parent.display().to_string(),
            source: e,
        })?;
    }

    let tmp_path = archive_path.with_extension("tar.zst.tmp");

    let file = std::fs::File::create(&tmp_path).map_err(|e| ArchiveError::Io {
        path: tmp_path.display().to_string(),
        source: e,
    })?;

    let encoder = zstd::Encoder::new(file, ZSTD_LEVEL).map_err(|e| ArchiveError::Io {
        path: tmp_path.display().to_string(),
        source: e,
    })?;

    let mut tar_builder = tar::Builder::new(encoder);

    // Add the directory contents under a flat prefix (no nested path)
    tar_builder
        .append_dir_all(".", dir)
        .map_err(|e| ArchiveError::Io {
            path: dir.display().to_string(),
            source: e,
        })?;

    let encoder = tar_builder.into_inner().map_err(|e| ArchiveError::Io {
        path: tmp_path.display().to_string(),
        source: e,
    })?;

    encoder.finish().map_err(|e| ArchiveError::Io {
        path: tmp_path.display().to_string(),
        source: e,
    })?;

    // Atomic rename
    std::fs::rename(&tmp_path, archive_path).map_err(|e| ArchiveError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let size = std::fs::metadata(archive_path)
        .map(|m| m.len())
        .unwrap_or(0);

    #[allow(clippy::cast_precision_loss)]
    let mb = size as f64 / 1_048_576.0;
    log::info!("  archive size: {mb:.1} MB");

    Ok(size)
}

/// Unpacks a `.tar.zst` archive into a directory.
///
/// The target directory is created if it does not exist. Existing
/// contents are **not** removed — the archive contents are extracted
/// on top.
///
/// # Errors
///
/// Returns an I/O error if the archive cannot be read or the
/// directory cannot be written.
pub fn unpack(archive_path: &Path, dest_dir: &Path) -> Result<u64, ArchiveError> {
    if !archive_path.exists() {
        return Err(ArchiveError::ArchiveNotFound(
            archive_path.display().to_string(),
        ));
    }

    log::info!(
        "Unpacking {} -> {}",
        archive_path.display(),
        dest_dir.display()
    );

    std::fs::create_dir_all(dest_dir).map_err(|e| ArchiveError::Io {
        path: dest_dir.display().to_string(),
        source: e,
    })?;

    let file = std::fs::File::open(archive_path).map_err(|e| ArchiveError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let decoder = zstd::Decoder::new(file).map_err(|e| ArchiveError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let mut archive = tar::Archive::new(decoder);

    let mut count = 0u64;
    archive.unpack(dest_dir).map_err(|e| ArchiveError::Io {
        path: dest_dir.display().to_string(),
        source: e,
    })?;

    // Count files in the extracted directory
    for entry in std::fs::read_dir(dest_dir).map_err(|e| ArchiveError::Io {
        path: dest_dir.display().to_string(),
        source: e,
    })? {
        let _ = entry;
        count += 1;
    }

    log::info!("  extracted {count} entries");

    Ok(count)
}

/// Errors from archive operations.
#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    /// Source directory not found.
    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),

    /// Archive file not found.
    #[error("Archive not found: {0}")]
    ArchiveNotFound(String),

    /// I/O error during pack/unpack.
    #[error("I/O error at {path}: {source}")]
    Io {
        /// Path that caused the error.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn pack_and_unpack_roundtrip() {
        let tmp = std::env::temp_dir().join("geocoder_index_archive_test");
        let _ = fs::remove_dir_all(&tmp);

        let src_dir = tmp.join("source");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(src_dir.join("test.txt"), b"hello world").unwrap();
        fs::write(src_dir.join("data.bin"), b"\x00\x01\x02\x03").unwrap();

        let archive = tmp.join("test.tar.zst");
        let size = pack(&src_dir, &archive).unwrap();
        assert!(size > 0);
        assert!(archive.exists());

        let dest_dir = tmp.join("unpacked");
        let count = unpack(&archive, &dest_dir).unwrap();
        assert!(count > 0);
        assert_eq!(
            fs::read_to_string(dest_dir.join("test.txt")).unwrap(),
            "hello world"
        );
        assert_eq!(
            fs::read(dest_dir.join("data.bin")).unwrap(),
            b"\x00\x01\x02\x03"
        );

        // Cleanup
        let _ = fs::remove_dir_all(&tmp);
    }
}
