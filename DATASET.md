## Benchmark Dataset

The benchmark dataset represents the existing Aerospike data in:

- Namespace: `smf`
- Set: `session`
- Current record count: approximately `57,250`
- Each record contains both indexed and non-indexed bins.
- The dataset schema must include **all bins present in the records**, not only bins with Secondary Indexes.

### Record Schema

A representative record is:

```json
{
  "Dnn": "Dnn_95",
  "IPType": "IPType_0",
  "State": "State_2",
  "Timer": 3,
  "SEID": "SEID_995",
  "startTime": 1704449815,
  "attachTime": 1707063660,
  "FTEIDC": "FTEIDC_865",
  "IP": "IP_158",
  "SNSSAI": "SNSSAI_2",
  "UpfSelection": 1709374722,
  "payload": "zxhjwtaxrtyhilcdjkksvwajrxcjrihdfycygcvpuqhdzajbxlxdmhmtdsixlkamoglnmxpsqfgcqpfsajxqhxcsulqmpsvoxjblpnhmomeqtkqxhwdrwvfyrhezsmlwidwiwsataydqtljiipbptlvkriphpnnjexijozcqyrtfzfzzdklfslaomnglukbdiitzpvzvmppmejtadvodfevddvdclemojdjjamyxequghihmxwvcebqbcrauj",
  "TEIDC": "TEIDC_196"
}
```

### Secondary Indexes

The following bins have Secondary Indexes:

```text
FTEIDC       STRING
TEIDC        STRING
SEID         STRING
Dnn          STRING
IPType       STRING
IP           STRING
State        STRING
SNSSAI       STRING
UpfSelection NUMERIC
Timer        NUMERIC
attachTime   NUMERIC
startTime    NUMERIC
```

`payload` is a non-indexed bin and must still be included in the benchmark dataset.

### Dataset Requirements

When generating or loading benchmark data:

1. Preserve the above record structure and data types.
2. Include both indexed and non-indexed bins.
3. Preserve realistic value distributions and cardinalities where production data is available.
