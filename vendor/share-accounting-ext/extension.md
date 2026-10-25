# 0. Abstract

Right now, payout schemes for miners account only for provided hash power. One of
the biggest bitcoin issue is the centralization of transaction selection, this issue is solved
by Stratum v2. Given that Stratum v2 let the miner select their own transaction
(this feature is known as Job Declaration), and that transactions fees might become the
most important part of the mining reward, we need a system that calculate the payouts
based both on selected transactions and hash power provided.

This document proposes an extension of StratumV2, that can be used by the miner to verify pool's
payout. Through this sv2 extension is possible to implement the system proposed in 
[here](https://www.dmnd.work/pplns-with-job-declaration/pplns-with-job-declaration.pdf).

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", 
"RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC2119.

# 1. Extension Overview

This extension assume that every miner of the pool mine on top of the same block, in case of a fork
every miner have to be on the same branch, this is also the right thing to do.

This extension let miners verify the pool's reward for a particular window. A miner is expected to
follow the below step in order to do that:

0. ask for the window that want to verify
1. randomly select some slice that he is willing to check
for each selected slice:
    1. randomly select some share in the slice
    2. fetch the transactions that are not in the cache for each selected share
    3. verify that each share is valid
    4. verify that merkle path of each share + share hash = root in the Slice
    5. verify that the sum of the diff verified shares is not bigger then the Slice diff
    6. verify that the fees in the shares are lower than fees of the Slice ref job fees + delta

## 1.1 Data Types
This extension introduce 3 datatypes:

### Slice

A slice represents a group of shares mined when the mempool's maximum extractable fees can be 
approximated as a constant.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| number_of_shares | U32       | How many shares are in the slice  |
| difficulty | U64  | sum of all the difficulties of the shares that compose the slice |
| fees | U64  | fees of the ref job for this slice |
| root | Hash256  | merkle root of the tree composed by all the shares in the slice |
| job_id | U64  | id of the ref job for this slice, the last 4 bytes MUST be the same of the job_id of the respective job |

### Share

A share sent by a miner

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| nonce | U32       | Same as in sv2 mining protocol  |
| ntime | U32       | Same as in sv2 mining protocol  |
| version | U32       | Same as in sv2 mining protocol  |
| extranonce | B032       | Same as in sv2 mining protocol  |
| job_id | U64  | id of the job for this slice, the last 4 bytes MUST be the same of the job_id of the respective job |
| reference_job_id | U64  | id of the ref job for this slice, the last 4 bytes MUST be the same of the job_id of the respective job |
| share_index | U32  | index of the share relative to the slice |
| merkle_path | B064K       | path of this share relative to the slice root  |

### PHash

The PHash message includes the previous hash and the starting index of the slices that use it. This
message is sent within the GetWindowSuccess response, enabling the miner to identify which slices 
correspond to which previous hash, and thereby determine the previous hash for each share. This
information is essential for verifying the work of each share.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| phash | Hash256       | The previous hash (32-byte binary value)  |
| index_start | u32  | The index of the first share in the window that use this `phash` |

This extension is an extension of the Mining StratumV2 protocol and message defined here MUST be
sent over an already setup mining connection. 

## 1.2 Activate Extension

TODO this mechanism is discussed [here](https://github.com/stratum-mining/sv2-spec/issues/95),
change the below with whatever we will agreed on.

After that a valid mining connection have been opened with upstream, the client that want to use this
Extension MUST send Activate and the pool that support this extension MUST answer with Activate.Success.
If the pool respond with ` Frame { extension_type, msg_type: 0xff, msg_length: 0, payload: [] }` 
client MUST not send any other messages.

## 1.3 Error

This extension handles errors differently compared to SV2. Instead of using several general errors
for different messages, each message with a handleable error defines a specific response, such as
`GetWindow` with `GetWindow.Busy`. There is only one general error, `ErrorMessage`, which is used for all
irrecoverable errors. It contains a string and MUST be used solely for logging the cause of the error. 
Upon receiving an `ErrorMessage`, the node MUST close the connection.

# 2. Extension Messages

## Activate (Client -> Server)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |

## Activate.Success (Server -> Client)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |

## ShareOk (Server -> Client)

Sent by the pool for each `SubmitShare` submitted by the pool. When this extension is active the pool
MAY not send `SubmitShares.Success`

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| ref_job_id | U64  | id of the ref job for this slice, the last 4 bytes MUST be the same of the job_id of the respective job |
| share_index | U32  | index of this share inside the slice |

## NewBlockFound (Server -> Client)

The pool must send `NewBlockFound` when a miner found a valid block.
this is different from SNPH since is only sent for block found by the pool not for every block.
`NewBlockFound.block_hash` is used by the miner to request a window through `GetWindow`. 

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| block_hash | U256  | block hash of the block that have been found by the pool |

## GetWindow (Client -> Server)

Pools should reply with `GetWindow.Success` for last windows, for older window pool MAY
answer with `GetWindow.Busy`. Pool must not reply with `GeneralError` for valid request a valid
request is a request that have as block_hash the hash of a block found by the pool.


| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |
| block_hash | U256  | Block hash of the block that have been found by the pool, for which we are requiring the window |

## GetWindowSuccess (Server -> Client)

The `GetWindowSuccess` message is sent from the server to the client in response to a `GetWindow` 
request, indicating that the requested window of slices has been successfully retrieved.

### Slice Definitions

* **First Slice (slice[start])**: This is the first slice in the window where the cumulative difficulty
  from slice[start] to slice[end - 1] meets or exceeds N × window_size. Formally:
  ```python
  sum(slice[start].diff, ..., slice[end - 1].diff) ≥ N × window_size

* **Last Slice (slice[end])**: This is the slice that contains the share which found a valid block.
  Note that this last slice is not used in reward calculations.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier to match the response with its corresponding request |
| slices | SEQ0_64K[SLICE]  | list of all the slices included in the window |
| phashes | SEQ0_64K[PHASH]  | List of all previous hashes (`phash`) in the window, along with the index of the first slice that uses each `phash` |

## GetWindowBusy (Server -> Client)

Client MUST not retry the request before retry_in_seconds seconds.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |
| retry_in_seconds | U64  | how many second client MUST wait before retry the request |

## GetShares (Client -> Server)

Client MUST NOT try to get shares that do not belong to the window that the client required, 
and pool authorized with `GetWindow.Success`. If so upstream SHOULD send an error and close the connection.

Share id is calculated from the first share in the first slice that have id = 0 to the last share in
the last slice that have id = shares_in_window.len() - 1.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response, this is the id of the initial GetWindow request |
| shares | SEQ0_64K[U32]  | List of the shares id that we want to fetch from the pool, id are relative to the window  |

## GetSharesSuccess (Server -> Client)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| shares | SEQ0_64K[SHARE]  | List of shares  |

## GetTransactionsInJob (Client -> Server)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| window_req_id | U32  | unique identifier of the original GetWindow request |
| request_id | U32  | Unique identifier for pairing the response |
| job_id | U64  | id of the job for which we are requiring the transactions, the last 4 bytes MUST be the same of the job_id of the respective job |

## GetTransactionsInJobSuccess (Server -> Client)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |
| coinbase_id | U256  | coinbase id of this job, this is needed to reconstruct the root  |
| tx_short_hash_nonce | U64  | A unique nonce used to ensure tx_short_hash collisions are uncorrelated across the network  |
| tx_short_hash_list | SEQ0_64K[SHORT_TX_ID]  | Sequence of SHORT_TX_IDs. Inputs to the SipHash functions are transaction hashes from the mempool. Secret keys k0, k1 are derived from the first two little-endian 64-bit integers from the SHA256(tx_short_hash_nonce), respectively (see bip-0152 for more information). Upstream node checks the list against its mempool. Does not include the coinbase transaction (as there is no corresponding full data for it yet).  |
| tx_hash_list_hash | U256  | Hash of the full sequence of SHA256(transaction_data) contained in the transaction_hash_list  |

## IdentifyTransaction (Client -> Server)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |

## IdentifyTransactionSuccess (Server -> Client)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |
| tx_data_hashes | SEQ0_64K[U256]  | The full list of transaction data hashes used to build the mining job in the corresponding DeclareMiningJob message |

## ProvideMissinTransactionsSuccess (Server -> Client)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| request_id | U32  | Unique identifier for pairing the response |
| transaction_list | SEQ0_64K[B0_16M]  | List of full transactions as requested by ProvideMissingTransactions, in the order they were requested in ProvideMissingTransactions |


## NewTxs (Server -> Client)

Pool MUST send new transactions that increase the MMEF to the miners. If a miner constantly get this
transactions before the pool MAY change pool.

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| transactions | SEQ0_64K[B0_16M]  | List of full transactions |

## ErrorMessage (Server -> Client, Client -> Server)

| Field Name | Data Type | Description                                                 |
| ---------- | --------- | ----------------------------------------------------------- |
| message | STR0255  | error message |

# 3. Message Types

| Message Type (8-bit)           | channel_msg bit | Message Name                       |
| ------------------------------ | --------------- | ---------------------------------- |
| 0x00                           | 0               | Activate                           |
| 0x01                           | 0               | Activate.Success                   |
| 0x02                           | 0               | ShareOk                            |
| 0x03                           | 0               | NewBlockFound                      |
| 0x04                           | 0               | GetWindow                          |
| 0x05                           | 0               | GetWindow.Success                  |
| 0x06                           | 0               | GetWindow.Busy                     |
| 0x07                           | 0               | GetShares                          |
| 0x08                           | 0               | GetShares.Success                  |
| 0x09                           | 0               | GetTransactionsInJob               |
| 0x0A                           | 0               | GetTransactionsInJob.Success       |
| 0x0B                           | 0               | IdentifyTransaction                |
| 0x0C                           | 0               | IdentifyTransaction.Success        |
| 0x0D                           | 0               | ProvideMissingTransactions         |
| 0x0E                           | 0               | ProvideMissingTransactions.Success |
| 0x0F                           | 0               | NewTxs                             |
| 0x10                           | 0               | ErrorMessage                       |
