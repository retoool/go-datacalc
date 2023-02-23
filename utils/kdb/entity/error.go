package entity

import "errors"

var (
	// Metric Errors.
	ErrorMetricNameInvalid = errors.New("Metric name empty")
	ErrorTagNameInvalid    = errors.New("Tag name empty")
	ErrorTagValueInvalid   = errors.New("Tag value empty")
	ErrorTTLInvalid        = errors.New("TTL value invalid")

	// Data Point Errors.
	ErrorDataPointInt64   = errors.New("Not an int64 data value")
	ErrorDataPointFloat32 = errors.New("Not a float32 data value")
	ErrorDataPointFloat64 = errors.New("Not a float64 data value")

	// Query Metric Errors.
	ErrorQMetricNameInvalid     = errors.New("Query Metric name empty")
	ErrorQMetricTagNameInvalid  = errors.New("Query Metric Tag name empty")
	ErrorQMetricTagValueInvalid = errors.New("Query Metric Tag value empty")
	ErrorQMetricLimitInvalid    = errors.New("Query Metric Limit must be >= 0")

	// Query Builder Errors.
	ErrorAbsRelativeStartSet      = errors.New("Both absolute and relative start times cannot be set")
	ErrorRelativeStartTimeInvalid = errors.New("Relative start time duration must be > 0")
	ErrorAbsRelativeEndSet        = errors.New("Both absolute and relative end times cannot be set")
	ErrorRelativeEndTimeInvalid   = errors.New("Relative end time duration must be > 0")
	ErrorStartTimeNotSpecified    = errors.New("Start time not specified")
)
