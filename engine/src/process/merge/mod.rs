mod boundary;
mod checkpoint;
mod commit;
mod depth_cursor;
mod depth_stream;
mod event_validation;
mod inventory;
mod kway;
mod manifest;
mod ordering;
mod resolver;
mod timeline;
mod writer;

pub use boundary::{DepthBoundaryLink, MergeBoundaryRelation, reconcile_depth_boundaries};
pub use checkpoint::{
    MergeCheckpoint, build_merge_checkpoints, resolve_initial_checkpoints, select_merge_checkpoints,
};
pub use commit::MergeCommitPaths;
pub use depth_cursor::DepthEventCursor;
pub use depth_stream::{DepthEventRef, DepthStream};
pub use event_validation::{DepthEventValidation, validate_depth_events};
pub use inventory::{DepthTaskInventory, build_depth_inventory};
pub use kway::{KWayDepthMerger, MergedDepthEvent};
pub use manifest::{
    MERGE_MANIFEST_FILENAME, MERGE_MANIFEST_VERSION, MergeBoundaryRecord, MergeInputDataset,
    MergeManifest, MergeSkippedInitialization, MergeStream, MergeTimelineFile, build_merge_streams,
    write_merge_manifest,
};
pub use ordering::MergeKey;
pub use resolver::{ResolvedMergeInput, resolve_merge_inputs};
pub use timeline::{TimelineBatchBuilder, TimelineRecord, timeline_schema};
pub use writer::{TimelineWriter, TimelineWriterMetrics};
