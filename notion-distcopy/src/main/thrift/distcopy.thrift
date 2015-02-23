namespace java com.ambiata.notion.distcopy

struct UploadMappingLookup {
    1: string path;
    2: string bucket;
    3: string key;
}

struct DownloadMappingLookup {
    1: string bucket;
    2: string key;
    3: string path;
}

union MappingLookup {
    1: UploadMappingLookup upload;
    2: DownloadMappingLookup download;
}

struct MappingsLookup {
    1: list<MappingLookup> mappings;
}
