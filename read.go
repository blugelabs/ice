//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ice

import (
	"encoding/binary"
)

func (s *Segment) getDocStoredMetaAndUnCompressed(docNum uint64) (meta, data []byte, err error) {
	_, storedOffset, err := s.getDocStoredOffsetsOnly(docNum)
	if err != nil {
		return nil, nil, err
	}

	// document chunk coder
	chunkI := docNum / uint64(defaultDocumentChunkSize)
	storedFieldDecompressed := s.decompressedStoredFieldChunks[chunkI]
	storedFieldDecompressed.m.Lock()
	if storedFieldDecompressed.data == nil {
		// we haven't already loaded and decompressed this chunk
		chunkOffsetStart := s.storedFieldChunkOffsets[int(chunkI)]
		chunkOffsetEnd := s.storedFieldChunkOffsets[int(chunkI)+1]
		compressed, err := s.data.Read(int(chunkOffsetStart), int(chunkOffsetEnd))
		if err != nil {
			return nil, nil, err
		}

		// decompress it
		storedFieldDecompressed.data, err = ZSTDDecompress(nil, compressed)
		if err != nil {
			return nil, nil, err
		}
	}
	storedFieldDecompressed.m.Unlock()

	storedFieldDecompressed.m.RLock()
	metaLenData := storedFieldDecompressed.data[int(storedOffset):int(storedOffset+binary.MaxVarintLen64)]
	var n uint64
	metaLen, read := binary.Uvarint(metaLenData)
	n += uint64(read)

	dataLenData := storedFieldDecompressed.data[int(storedOffset+n):int(storedOffset+n+binary.MaxVarintLen64)]
	dataLen, read := binary.Uvarint(dataLenData)
	n += uint64(read)

	meta = storedFieldDecompressed.data[int(storedOffset+n):int(storedOffset+n+metaLen)]
	data = storedFieldDecompressed.data[int(storedOffset+n+metaLen):int(storedOffset+n+metaLen+dataLen)]
	storedFieldDecompressed.m.RUnlock()
	return meta, data, nil
}

func (s *Segment) getDocStoredOffsetsOnly(docNum uint64) (indexOffset, storedOffset uint64, err error) {
	indexOffset = s.footer.storedIndexOffset + (fileAddrWidth * docNum)
	storedOffsetData, err := s.data.Read(int(indexOffset), int(indexOffset+fileAddrWidth))
	if err != nil {
		return 0, 0, err
	}
	storedOffset = binary.BigEndian.Uint64(storedOffsetData)
	return indexOffset, storedOffset, nil
}
