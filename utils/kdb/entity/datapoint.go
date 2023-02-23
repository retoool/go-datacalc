package entity

import (
	"encoding/json"
	"errors"
)

// Represents a measurement. Stores the time when the measurement occurred and its value.
type DataPoint struct {
	timestamp int64
	value     interface{}
}

func NewDataPoint(ts int64, val interface{}) *DataPoint {
	return &DataPoint{
		timestamp: ts,
		value:     val,
	}
}

func (dp *DataPoint) Timestamp() int64 {
	return dp.timestamp
}

func (dp *DataPoint) Int64Value() (int64, error) {
	val, ok := dp.value.(int64)
	if !ok {
		v, ok := dp.value.(int)
		if !ok {
			return 0, ErrorDataPointInt64
		}
		val = int64(v)
	}

	return val, nil
}

func (dp *DataPoint) Float64Value() (float64, error) {
	val, ok := dp.value.(float64)
	if !ok {
		return 0, ErrorDataPointFloat64
	}
	return val, nil
}

// 20191101 add by wutz (no need)
func (dp *DataPoint) Float32Value() (float32, error) {
	val, ok := dp.value.(float32)
	if !ok {
		return 0, ErrorDataPointFloat32
	}
	return val, nil
}

func (dp *DataPoint) MarshalJSON() ([]byte, error) {
	data := []interface{}{dp.timestamp, dp.value}
	return json.Marshal(data)
}

func (dp *DataPoint) UnmarshalJSON(data []byte) error {
	var arr []interface{}
	err := json.Unmarshal(data, &arr)
	if err != nil {
		return err
	}

	var v float64
	ok := false
	if v, ok = arr[0].(float64); !ok {
		return errors.New("Invalid Timestamp type")
	}

	// Update the receiver with the values decoded.
	dp.timestamp = int64(v)
	dp.value = arr[1]

	return nil
}
