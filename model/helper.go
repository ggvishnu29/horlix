package model

func CloneDataSlice(srcDataSlice [][]byte) [][]byte {
	var destDataSlice [][]byte
	for _, data := range srcDataSlice {
		data1 := make([]byte, len(data))
		copy(data1, data)
		destDataSlice = append(destDataSlice, data1)
	}
	return destDataSlice
}
