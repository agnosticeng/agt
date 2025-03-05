create table buffer_{{.RANGE_START}}_{{.RANGE_END}} engine=TinyLog
as (
    with
        {{.RANGE_START}} as start,
        {{.RANGE_END}} as end,

        block_numbers as (
            select 
                generate_series as n 
            from generate_series(start, end)
        )

    select 
        toDateTime64(evm_hex_decode_int(b.timestamp, 'Int64'), 3, 'UTC') as timestamp,
        evm_hex_decode_int(b.baseFeePerGas, 'UInt256') as base_fee_per_gas,
        evm_hex_decode_int(b.blobGasUsed, 'UInt64') as blob_gas_used,
        evm_hex_decode_int(b.difficulty, 'UInt256') as difficulty,
        evm_hex_decode_int(b.excessBlobGas, 'UInt64') as excess_blob_gas,
        evm_hex_decode(b.extraData) as extra_data,
        evm_hex_decode_int(b.gasLimit, 'UInt64') as gas_limit,
        evm_hex_decode_int(b.gasUsed, 'UInt64') as gas_used,
        evm_hex_decode(b.hash) as hash,
        evm_hex_decode(b.miner) as miner,
        evm_hex_decode(b.mixHash) as mix_hash,
        evm_hex_decode_int(b.nonce, 'UInt256') as nonce,
        evm_hex_decode_int(b.number, 'UInt64') as number,
        evm_hex_decode(b.parentBeaconBlockRoot) as parent_beacon_block_root,
        evm_hex_decode(b.parentHash) as parent_hash,
        evm_hex_decode(b.receiptsRoot) as receipts_root,
        evm_hex_decode(b.sha3Uncles) as sha3_uncles,
        evm_hex_decode_int(b.size, 'UInt32') as size,
        evm_hex_decode(b.stateRoot) as state_root,
        evm_hex_decode_int(b.totalDifficulty, 'UInt256') as total_difficulty,
        evm_hex_decode(b.transactionsRoot) as transactions_root,
        arrayMap(x -> evm_hex_decode(x), b.uncles) as uncles,
        evm_hex_decode(b.withdrawalsRoot) as withdrawals_root
    from (
        select
            JSONExtract(
                ethereum_rpc(
                    'eth_getBlockByNumber', 
                    [evm_hex_encode_int(n), 'false'], 
                    '{{.RPC_ENDPOINT}}#fail-on-error=true&fail-on-null=true'
                ),
                'value',
                'Tuple(
                    timestamp String,
                    baseFeePerGas String,
                    blobGasUsed String,
                    difficulty String,
                    excessBlobGas String,
                    extraData String,
                    gasLimit String,
                    gasUsed String,
                    hash String,
                    miner String,
                    mixHash String,
                    nonce String,
                    number String,
                    parentBeaconBlockRoot String,
                    parentHash String,
                    receiptsRoot String,
                    sha3Uncles String,
                    size String,
                    stateRoot String,
                    totalDifficulty String,
                    transactionsRoot String,
                    uncles Array(String),
                    withdrawalsRoot String
                )'
            ) as b
        from block_numbers
    )
)