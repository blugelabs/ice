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
	s.m.Lock()
	var storedFieldDecompressed []byte
	var ok bool
	if storedFieldDecompressed, ok = s.decompressedStoredFieldChunks[chunkI]; !ok {
		// we haven't already loaded and decompressed this chunk
		chunkOffsetStart := s.storedFieldChunkOffsets[int(chunkI)]
		chunkOffsetEnd := s.storedFieldChunkOffsets[int(chunkI)+1]
		compressed, err := s.data.Read(int(chunkOffsetStart), int(chunkOffsetEnd))
		if err != nil {
			s.m.Unlock()
			return nil, nil, err
		}

		// decompress it
		storedFieldDecompressed, err = ZSTDDecompress(nil, compressed)
		if err != nil {
			s.m.Unlock()
			return nil, nil, err
		}

		// store it
		s.decompressedStoredFieldChunks[chunkI] = storedFieldDecompressed
	}
	s.m.Unlock()

	metaLenData := storedFieldDecompressed[int(storedOffset):int(storedOffset+binary.MaxVarintLen64)]
	var n uint64
	metaLen, read := binary.Uvarint(metaLenData)
	n += uint64(read)

	dataLenData := storedFieldDecompressed[int(storedOffset+n):int(storedOffset+n+binary.MaxVarintLen64)]
	dataLen, read := binary.Uvarint(dataLenData)
	n += uint64(read)

	meta = storedFieldDecompressed[int(storedOffset+n):int(storedOffset+n+metaLen)]
	data = storedFieldDecompressed[int(storedOffset+n+metaLen):int(storedOffset+n+metaLen+dataLen)]
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
