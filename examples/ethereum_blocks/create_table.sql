attach table if not exists {{.CHAIN}}_blocks uuid '{{.TABLE_UUID}}' (
    timestamp DateTime64(3, 'UTC') CODEC(ZSTD),
    base_fee_per_gas UInt256 CODEC(ZSTD),
    blob_gas_used UInt64 CODEC(ZSTD),
    difficulty UInt256 CODEC(ZSTD),
    excess_blob_gas UInt64 CODEC(ZSTD),
    extra_data String CODEC(ZSTD),
    gas_limit UInt64 CODEC(ZSTD),
    gas_used UInt64 CODEC(ZSTD),
    hash FixedString(32) CODEC(ZSTD),
    miner FixedString(20) CODEC(ZSTD),
    mix_hash FixedString(32) CODEC(ZSTD),
    nonce UInt256 CODEC(ZSTD),
    number UInt64 CODEC(ZSTD),
    parent_beacon_block_root FixedString(32) CODEC(ZSTD),
    parent_hash FixedString(32) CODEC(ZSTD),
    receipts_root FixedString(32) CODEC(ZSTD),
    sha3_uncles FixedString(32) CODEC(ZSTD),
    size UInt32 CODEC(ZSTD),
    state_root FixedString(32) CODEC(ZSTD),
    total_difficulty UInt256 CODEC(ZSTD),
    transactions_root FixedString(32) CODEC(ZSTD),
    uncles Array(FixedString(32)) CODEC(ZSTD),
    withdrawals_root FixedString(32) CODEC(ZSTD),

    index idx_timestamp timestamp type minmax granularity 1,
    index idx_hash hash type bloom_filter granularity 4,
    index idx_miner miner type bloom_filter granularity 4
)
engine = ReplacingMergeTree
partition by toYYYYMM(timestamp)
order by (number)
settings 
    disk = disk(
        type=cache,
        max_size='{{.CACHE_MAX_SIZE}}',
        path='{{.CACHE_PATH}}',
        disk = disk(
            type=s3,
            endpoint='{{.S3_ENDPOINT}}',
            region='{{.S3_REGION}}',
            access_key_id='{{.S3_ACCESS_KEY_ID}}',
            secret_access_key='{{.S3_SECRET_ACCESS_KEY}}',
            use_environment_credentials=0,
            metadata_type=plain_rewritable,
            readonly=false
        )
    ),
    min_bytes_for_wide_part=4294967296,
    min_rows_for_wide_part=1000000000